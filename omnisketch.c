/*
 * omnisketch.c - implementation of OmniSketch for PostgreSQL, useful for
 * counting with filtering. Described in paper
 *
 * OmniSketch: Efficient Multi-Dimensional High-Velocity Stream Analytics
 * with Arbitrary Predicates; Wieger R. Punter, Odysseas Papapetrou,
 * Minos Garofalakis [arXiv:2309.06051]
 *
 * Copyright (C) Tomas Vondra, 2024
 */

#include <math.h>
#include <xxh3.h>

#include "postgres.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "common/pg_prng.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/typcache.h"

PG_MODULE_MAGIC;

/* representation of the whole omnisketch sketch */
typedef struct omnisketch_t
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		flags;			/* reserved for future use (versioning, ...) */
	int16		numSketches;	/* number of per-attribute sketches */
	int16		sketchWidth;	/* number of "columns" of sketches */
	int16		sketchHeight;	/* number of "rows" of sketches */
	int16		sampleSize;		/* sample size for each bucket */
	int16		itemSize;		/* item size (for the sample) */
	int32		count;			/* number of entries added */
	uint32		seed;			/* used for generating "unique" items */

	/* followed by data (counts/samples) for each sketch */
} omnisketch_t;

/* single bucket of a sketch */
typedef struct bucket_t
{
	uint32		totalCount;		/* total entries represented by bucket */
	uint16		sampleCount;	/* number of entries in the sample */
	uint16		maxIndex;		/* index on the entry with the max hash */
	uint32		maxHash;		/* maximum hash */
	bool		isSorted;		/* is bucket sample sorted (by hash) */
} bucket_t;

/* array of item IDs (not necessarily ordered) */
typedef struct item_list_t
{
	uint32		nitems;
	int32		items[FLEXIBLE_ARRAY_MEMBER];
} item_list_t;

/* item and a hash of the item */
typedef struct item_hash_t {
	int32		item;
	uint32		hash;
} item_hash_t;

#define SKETCH_SIZE(s)	\
	((s)->sketchWidth * (s)->sketchHeight)

#define SKETCH_BUCKETS(s)	\
	((bucket_t *) ((char *) (s) + \
		MAXALIGN(sizeof(omnisketch_t))))

#define SKETCH_SAMPLES(s)	\
	((int32 *) ((char *) (s) + \
		MAXALIGN(sizeof(omnisketch_t)) + \
		MAXALIGN(SKETCH_SIZE(s) * (s)->numSketches * sizeof(bucket_t))))

#define SKETCH_BUCKET_INDEX(s, a, i, j)	\
		((a) * SKETCH_SIZE(s) + (i) * (s)->sketchWidth + (j))

#define SKETCH_BUCKET(s, a, i, j)	\
	(&(SKETCH_BUCKETS(s))[SKETCH_BUCKET_INDEX(s, a, i, j)])

#define SKETCH_SAMPLE(s, a, i, j)	\
	(&SKETCH_SAMPLES(s)[SKETCH_BUCKET_INDEX(s, a, i, j) * (s)->sampleSize])

#define SKETCH_SEED			0xFFFFFFFF
#define SKETCH_HASH(v, s)	(uint32) XXH32(&(v), sizeof(uint32), (s))

/* prototypes */
PG_FUNCTION_INFO_V1(omnisketch_add);

PG_FUNCTION_INFO_V1(omnisketch_combine);
PG_FUNCTION_INFO_V1(omnisketch_finalize);

PG_FUNCTION_INFO_V1(omnisketch_in);
PG_FUNCTION_INFO_V1(omnisketch_out);
PG_FUNCTION_INFO_V1(omnisketch_send);
PG_FUNCTION_INFO_V1(omnisketch_recv);

PG_FUNCTION_INFO_V1(omnisketch_count);
PG_FUNCTION_INFO_V1(omnisketch_estimate);
PG_FUNCTION_INFO_V1(omnisketch_text);
PG_FUNCTION_INFO_V1(omnisketch_json);

Datum omnisketch_add(PG_FUNCTION_ARGS);

Datum omnisketch_combine(PG_FUNCTION_ARGS);
Datum omnisketch_finalize(PG_FUNCTION_ARGS);

Datum omnisketch_in(PG_FUNCTION_ARGS);
Datum omnisketch_out(PG_FUNCTION_ARGS);
Datum omnisketch_send(PG_FUNCTION_ARGS);
Datum omnisketch_recv(PG_FUNCTION_ARGS);

Datum omnisketch_count(PG_FUNCTION_ARGS);
Datum omnisketch_estimate(PG_FUNCTION_ARGS);
Datum omnisketch_text(PG_FUNCTION_ARGS);
Datum omnisketch_json(PG_FUNCTION_ARGS);

#define EULER_NUMBER	2.71828

/*
 * Get a valid omnisketch struct, with full 4B header. If not needed, use
 * just plain PG_GETARG_POINTER.
 */
#define PG_GETARG_OMNISKETCH(x)	(omnisketch_t *) PG_DETOAST_DATUM(PG_GETARG_DATUM(x))

#ifdef USE_ASSERT_CHECKING
static void
AssertCheckBucketBasic(omnisketch_t *sketch, bucket_t *bucket)
{
	Assert(bucket->totalCount >= bucket->sampleCount);
	Assert(bucket->totalCount <= sketch->count);
	Assert(bucket->sampleCount <= sketch->sampleSize);
	Assert(bucket->maxIndex <= bucket->sampleCount);	/* count can be 0 */
}
#endif

/*
 * check info about bucket
 *
 * - items are expected to be sorted by hash, etc.
 * - the cached maximum hash value matches the actual value for one item
 * - there are not more than sampleSize items
 */
static void
AssertCheckBucket(omnisketch_t *sketch, bucket_t *bucket, int32 *sample)
{
#ifdef USE_ASSERT_CHECKING
	uint32		h_prev;

	/* check basic bucket info */
	AssertCheckBucketBasic(sketch, bucket);

	/*
	 * If there were any items added to the bucket, there has to be at least
	 * one sample too.
	 */
	Assert(!((bucket->sampleCount == 0) && (bucket->totalCount > 0)));

	/* If there are no samples, we're done. */
	if (bucket->sampleCount == 0)
		return;

	/* Make sure the max hash is valid (there's at least one item). */
	Assert(bucket->maxIndex < bucket->sampleCount);
	Assert(bucket->maxHash == SKETCH_HASH(sample[bucket->maxIndex], SKETCH_SEED));

	/* if sorted, the max hash is at the very end of the list */
	Assert(!bucket->isSorted || (bucket->maxIndex == (bucket->sampleCount - 1)));

	/*
	 * Check the max hash is really the max, and maybe also the ordering
	 * (if the items are sorted).
	 */
	for (int i = 0; i < bucket->sampleCount; i++)
	{
		uint32	h = SKETCH_HASH(sample[i], SKETCH_SEED);

		Assert(h <= bucket->maxHash);

		/* No previous item, or items are not sorted (yet), we're done. */
		if ((i > 0) && bucket->isSorted)
		{
			Assert((h > h_prev) ||
				   ((h == h_prev) && (sample[i] > sample[i-1])));
		}
		else if (i > 0)
		{
			Assert(sample[i] != sample[i-1]);
		}

		h_prev = h;
	}
#endif
}

/* basic checks on the OmniSketch (proper sum of counts, ...) */
static void
AssertCheckSketch(omnisketch_t *sketch)
{
#ifdef USE_ASSERT_CHECKING
	for (int a = 0; a < sketch->numSketches; a++)
	{
		for (int i = 0; i < sketch->sketchHeight; i++)
		{
			int64	count = 0;

			for (int j = 0; j < sketch->sketchWidth; j++)
			{
				bucket_t *bucket = SKETCH_BUCKET(sketch, a, i, j);

				AssertCheckBucketBasic(sketch, bucket);

				count += bucket->totalCount;
			}

			Assert(sketch->count == count);
		}
	}
#endif
}

/*
 * Allocate omnisketch with enough space for a requested number of items.
 *
 * FIXME This ignores the itemSize, and just uses 32bit IDs all the time.
 */
static omnisketch_t *
omnisketch_allocate(int nsketches, int width, int height, int sampleSize, int itemSize)
{
	omnisketch_t *sketch;

	/* header shared by all per-attribute sketches */
	Size	len = MAXALIGN(sizeof(omnisketch_t));

	/* counts */
	len += MAXALIGN(nsketches * width * height * sizeof(bucket_t));

	/* samples */
	len += MAXALIGN(nsketches * width * height * sizeof(int32) * sampleSize);

	/* make sure to zero the struct, to keep it compressible */
	sketch = palloc0(len);

	SET_VARSIZE(sketch, len);

	sketch->numSketches = nsketches;
	sketch->sketchWidth = width;
	sketch->sketchHeight = height;
	sketch->sampleSize = sampleSize;
	sketch->itemSize = itemSize;

	sketch->seed = pg_prng_uint32(&pg_global_prng_state);

	AssertCheckSketch(sketch);

	return sketch;
}

/*
 * Add an ID into the sample, possibly updating the max hash for the bucket.
 *
 * If the sample is not full (there are fewer than sampleSize items), add the
 * item into the array, and update the max hash value.
 *
 * If the sample is full, consider the hash value and only add the item if
 * the hash is less than the current max hash.
 *
 * Updates the sample size.
 *
 * FIXME make sure to reset the isSorted flag.
 */
static void
omnisketch_sample_add(omnisketch_t *sketch, bucket_t *bucket, int32 *sample, uint32 item)
{
	/*
	 * XXX is it necessary / good idea to use the seed? maybe the seed should
	 * be calculated from the parameters, to make it harder to construct an
	 * adversary data set?
	 *
	 * XXX The reason why the seed is not 0 or a small number, is that we use
	 * those seeds for calculating the buckets (columns), so it seems reasonable
	 * to use something different.
	 */
	uint32	h = SKETCH_HASH(item, SKETCH_SEED);

	/*
	 * Add the item to the sample, depending on the hash compared to the max
	 * hash for the bucket.
	 */
	if (bucket->sampleCount < sketch->sampleSize)
	{
		if (bucket->sampleCount == 0)
		{
			bucket->maxIndex = 0;
			bucket->maxHash = h;
		}
		else if (h > bucket->maxHash)
		{
			bucket->maxIndex = bucket->sampleCount;
			bucket->maxHash = h;
		}

		sample[bucket->sampleCount++] = item;

		AssertCheckBucket(sketch, bucket, sample);
	}
	else if (h < bucket->maxHash)
	{
		/*
		 * Replace the current max (because that's the one we want to get rid
		 * of), and calculate the new maximum. We don't know if the new item
		 * has the max, so we have to walk the whole array. Shouldn't be very
		 * common case, most items will have hash greater than the current max.
		 *
		 * FIXME if the hash is exactly the same (unlikely but possible), we
		 * should compare the them too, i.e. sort by (hash, item). Should
		 * make the behavior more consistent (but hash equality, aka collision,
		 * is a very rare case, so not very important).
		 */
		sample[bucket->maxIndex] = item;

		bucket->maxHash = 0;
		for (int k = 0; k < bucket->sampleCount; k++)
		{
			h = SKETCH_HASH(sample[k], SKETCH_SEED);

			if (h >= bucket->maxHash)
			{
				bucket->maxHash = h;
				bucket->maxIndex = k;
			}
		}

		AssertCheckBucket(sketch, bucket, sample);
	}
}

/*
 * Add hash and item to the bucket and the associated sample.
 */
static void
omnisketch_add_hash(omnisketch_t *sketch, int column, uint32 hash, uint32 item)
{
	for (int i = 0; i < sketch->sketchHeight; i++)
	{
		bucket_t   *bucket;
		int32	   *sample;

		uint32	h = (uint32) SKETCH_HASH(hash, i);
		int		j = (h % sketch->sketchWidth);

		Assert(i >= 0 && i < sketch->sketchHeight);
		Assert(j >= 0 && j < sketch->sketchWidth);

		bucket = SKETCH_BUCKET(sketch, column, i, j);
		sample = SKETCH_SAMPLE(sketch, column, i, j);

		bucket->totalCount++;

		omnisketch_sample_add(sketch, bucket, sample, item);
	}
}

/* add a value to the omnisketch */
Datum
omnisketch_add(PG_FUNCTION_ARGS)
{
	omnisketch_t   *sketch = NULL;
	uint32			id;
	HeapTupleHeader	record = PG_GETARG_HEAPTUPLEHEADER(3);

	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupdesc;
	HeapTupleData tuple;
	int			ncolumns;
	TypeCacheEntry **my_extra;
	Datum	   *values;
	bool	   *nulls;
	uint32		element_hash;

	/* Build temporary HeapTuple control structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(record);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = record;

	tupType = HeapTupleHeaderGetTypeId(record);
	tupTypmod = HeapTupleHeaderGetTypMod(record);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
	ncolumns = tupdesc->natts;

	/* make sure to have a sketch */
	if (PG_ARGISNULL(0))
	{
		int			w, d, B, b;
		double		epsilon, delta;

		/* */
		epsilon = PG_GETARG_FLOAT8(1);
		delta = PG_GETARG_FLOAT8(2);

		/* section 3.2 in the paper (configuring the sketch) */
		d = ceil(log(2.0 / delta));
		w = 1.0 + ceil(EULER_NUMBER * pow((epsilon + 1.0) / epsilon, 1.0 / d));

		/* pick the bucket/item sizes */
		B=0;
		b=0;
		while (b < 32 && B < 1024)
		{
			B += 1;
			b = ceil(log(4 * pow(B, 2.5) / delta));
		}

		sketch = omnisketch_allocate(ncolumns, w, d, B, b);
	}
	else
	{
		sketch = PG_GETARG_OMNISKETCH(0);
	}

	Assert(sketch != NULL);

	if (sketch->numSketches != ncolumns)
		elog(ERROR, "number of record attributes mismatches sketch (%d != %d)",
			 ncolumns, sketch->numSketches);

	/* increment the number of records added to the sketch */
	sketch->count++;
	id = XXH32(&sketch->count, sizeof(uint32), sketch->seed);

	my_extra = (TypeCacheEntry **) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL)
	{
		my_extra =
			MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt,
								   sizeof(TypeCacheEntry *) * ncolumns);
		fcinfo->flinfo->fn_extra = my_extra;
	}

	/* Break down the tuple into fields */
	values = (Datum *) palloc(ncolumns * sizeof(Datum));
	nulls = (bool *) palloc(ncolumns * sizeof(bool));
	heap_deform_tuple(&tuple, tupdesc, values, nulls);

	for (int i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute att;
		TypeCacheEntry *typentry;

		att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;

		/*
		 * Lookup the hash function if not done already
		 */
		typentry = my_extra[i];
		if (typentry == NULL ||
			typentry->type_id != att->atttypid)
		{
			typentry = lookup_type_cache(att->atttypid,
										 TYPECACHE_HASH_EXTENDED_PROC_FINFO);
			if (!OidIsValid(typentry->hash_extended_proc_finfo.fn_oid))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("could not identify an extended hash function for type %s",
								format_type_be(typentry->type_id))));
			my_extra[i] = typentry;
		}

		/* Compute hash of element */
		if (nulls[i])
		{
			// FIXME handle NULL
			element_hash = 0;
		}
		else
		{
			LOCAL_FCINFO(locfcinfo, 2);

			InitFunctionCallInfoData(*locfcinfo, &typentry->hash_extended_proc_finfo, 2,
									 att->attcollation, NULL, NULL);
			locfcinfo->args[0].value = values[i];
			locfcinfo->args[0].isnull = false;
			locfcinfo->args[1].value = Int64GetDatum(0);
			locfcinfo->args[0].isnull = false;
			element_hash = DatumGetUInt64(FunctionCallInvoke(locfcinfo));

			/* We don't expect hash support functions to return null */
			Assert(!locfcinfo->isnull);
		}

		omnisketch_add_hash(sketch, i, element_hash, id);
	}

	pfree(values);
	pfree(nulls);
	ReleaseTupleDesc(tupdesc);

	/* Avoid leaking memory when handed toasted input. */
	// PG_FREE_IF_COPY(record, 0);

	AssertCheckSketch(sketch);

	// FIXME
	PG_RETURN_POINTER(sketch);
}

/*
 * compare (hash, item) lexicographically - first by hash, then by item
 */
static int
cmp_item_hash(const void *a, const void *b)
{
	item_hash_t *ia = (item_hash_t *) a;
	item_hash_t *ib = (item_hash_t *) b;

	if (ia->hash < ib->hash)
		return -1;
	else if (ia->hash > ib->hash)
		return 1;

	if (ia->item < ib->item)
		return -1;
	else if (ia->item > ib->item)
		return 1;

	/* FIXME duplicate item IDs shouldn't really happen, right? */
	return 0;
}

/*
 * Returns the items sorted by hash. The elements are pairs (item,hash), so
 * that the hash does not need to be recalculated - both for sorting, and
 * later.
 */
static item_hash_t *
omnisketch_sorted_items(bucket_t *bucket, int32 *items)
{
	item_hash_t *sorted;

	sorted = palloc(bucket->sampleCount * sizeof(item_hash_t));

	for (int k = 0; k < bucket->sampleCount; k++)
	{
		sorted[k].item = items[k];
		sorted[k].hash = SKETCH_HASH(items[k], SKETCH_SEED);
	}

	if (!bucket->isSorted)
		pg_qsort(sorted, bucket->sampleCount, sizeof(item_hash_t), cmp_item_hash);

	return sorted;
}

/*
 * make sure samples for all buckets are sorted
 */
Datum
omnisketch_finalize(PG_FUNCTION_ARGS)
{
	omnisketch_t *sketch = PG_GETARG_OMNISKETCH(0);

	for (int i = 0; i < sketch->numSketches; i++)
	{
		for (int j = 0; j < sketch->sketchHeight; j++)
		{
			for (int k = 0; k < sketch->sketchWidth; k++)
			{
				bucket_t   *bucket = SKETCH_BUCKET(sketch, i, j, k);
				int32	   *sample = SKETCH_SAMPLE(sketch, i, j, k);

				/* nothing to do if already sorted */
				if (bucket->isSorted)
					continue;

				/*
				 * sort the item lists, so that we can calculate intersection
				 * more efficiently in _estimate
				 *
				 * XXX Not sure it's worth it to construct the separate array
				 * with hash. If the hash function is sufficiently cheap, maybe
				 * we should simply calculate the hashes ad hoc. Or maybe we
				 * could leverage knowing if the array is presorted? (say, once
				 * we add a new item into already-sorted array, we only replace
				 * the max hash, which is the last element of the sample array).
				 */
				if (bucket->sampleCount >= 2)
				{
					item_hash_t *items;

					items = omnisketch_sorted_items(bucket, sample);

					for (int l = 0; l < bucket->sampleCount; l++)
						sample[l] = items[l].item;

					pfree(items);

					/* the largest hash is at the very end */
					bucket->maxIndex = (bucket->sampleCount - 1);
					bucket->isSorted = true;

					AssertCheckBucket(sketch, bucket, sample);
				}
			}
		}
	}

	AssertCheckSketch(sketch);

	PG_RETURN_POINTER(sketch);
}

/* compare sketch parameters, make sure it's the same / compatible */
static bool
omnisketch_equals(omnisketch_t *a, omnisketch_t *b)
{
	return ((a->numSketches == b->numSketches) &&
			(a->sketchHeight == b->sketchHeight) &&
			(a->sketchWidth == b->sketchWidth) &&
			(a->sampleSize == b->sampleSize) &&
			(a->itemSize == b->itemSize));
}

/* copy the sketch into the current memory context */
static omnisketch_t *
omnisketch_copy(omnisketch_t *sketch)
{
	omnisketch_t   *dst;
	Size			len = VARSIZE_ANY(sketch);

	len = VARSIZE_ANY(sketch);
	dst = palloc(len);
	memcpy(dst, (char *) sketch, len);

	return dst;
}

/* merge two buckets, the first bucket is updated */
static void
omnisketch_merge_buckets(omnisketch_t *dst, omnisketch_t *src,
						 bucket_t *dst_bucket, bucket_t *src_bucket,
						 int32 *dst_sample, int32 *src_sample,
						 int sampleSize)
{
	int			i,
				j,
				k;
	item_hash_t *dst_items;
	item_hash_t *src_items;

	AssertCheckBucket(dst, dst_bucket, dst_sample);
	AssertCheckBucket(src, src_bucket, src_sample);

	/* nothing to do if the second bucket is empty */
	if (src_bucket->sampleCount == 0)
		return;

	dst_items = omnisketch_sorted_items(dst_bucket, dst_sample);
	src_items = omnisketch_sorted_items(src_bucket, src_sample);

	i = j = k = 0;
	while ((k < sampleSize) &&
		   (i < dst_bucket->sampleCount || j < src_bucket->sampleCount))
	{
		if (i == dst_bucket->sampleCount)
		{
			dst_sample[k++] = src_items[j].item;
			dst_bucket->maxHash = src_items[j++].hash;
			continue;
		}
		else if (j == src_bucket->sampleCount)
		{
			dst_sample[k++] = dst_items[i].item;
			dst_bucket->maxHash = dst_items[i++].hash;
			continue;
		}

		/* FIXME shouldn't really happen, I think, or maybe advance both? */
		Assert(cmp_item_hash(&dst_items[i], &src_items[j]) != 0);

		if (cmp_item_hash(&dst_items[i], &src_items[j]) > 0)
		{
			dst_sample[k++] = src_items[j].item;
			dst_bucket->maxHash = src_items[j++].hash;
		}
		else
		{
			dst_sample[k++] = dst_items[i].item;
			dst_bucket->maxHash = dst_items[i++].hash;
		}
	}

	pfree(dst_items);
	pfree(src_items);

	Assert(k >= Max(dst_bucket->sampleCount, src_bucket->sampleCount));

	dst_bucket->totalCount += src_bucket->totalCount;
	dst_bucket->isSorted = true;
	dst_bucket->sampleCount = k;
	dst_bucket->maxIndex = (dst_bucket->sampleCount - 1);

	AssertCheckBucket(dst, dst_bucket, dst_sample);
}

/*
 * Combine two sketches, the first sketch is updated with the result and then
 * returned (if NULL, a new sketch is allocated). Used as combine function for
 * parallel aggregates.
 */
Datum
omnisketch_combine(PG_FUNCTION_ARGS)
{
	omnisketch_t	 *src;
	omnisketch_t	 *dst;
	MemoryContext aggcontext;
	MemoryContext oldcontext;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "omnisketch_combine called in non-aggregate context");

	/* if no "merged" state yet, try creating it */
	if (PG_ARGISNULL(0))
	{
		/* nope, the second argument is NULL to, so return NULL */
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();

		/* the second argument is not NULL, so copy it */
		src = (omnisketch_t *) PG_GETARG_OMNISKETCH(1);

		/* copy the sketch into the right long-lived memory context */
		oldcontext = MemoryContextSwitchTo(aggcontext);
		dst = omnisketch_copy(src);
		MemoryContextSwitchTo(oldcontext);

		PG_RETURN_POINTER(dst);
	}

	/*
	 * If the second argument is NULL, just return the first one (we know
	 * it's not NULL at this point).
	 */
	if (PG_ARGISNULL(1))
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));

	/* Now we know neither argument is NULL, so merge them. */
	dst = (omnisketch_t *) PG_GETARG_OMNISKETCH(0);
	src = (omnisketch_t *) PG_GETARG_OMNISKETCH(1);

	AssertCheckSketch(dst);
	AssertCheckSketch(src);

	if (!omnisketch_equals(src, dst))
		elog(ERROR, "sketches do not match");

	for (int a = 0; a < src->numSketches; a++)
	{
		for (int i = 0; i < src->sketchHeight; i++)
		{
			for (int j = 0; j < src->sketchWidth; j++)
			{
				bucket_t *dst_bucket = SKETCH_BUCKET(dst, a, i, j);
				bucket_t *src_bucket = SKETCH_BUCKET(src, a, i, j);

				int32 *dst_sample = SKETCH_SAMPLE(dst, a, i, j);
				int32 *src_sample = SKETCH_SAMPLE(src, a, i, j);

				omnisketch_merge_buckets(dst, src,
										 dst_bucket, src_bucket,
										 dst_sample, src_sample,
										 dst->sampleSize);

				AssertCheckBucket(dst, dst_bucket, dst_sample);
				AssertCheckBucket(src, src_bucket, src_sample);
			}
		}
	}

	dst->count += src->count;

	AssertCheckSketch(dst);

	PG_RETURN_POINTER(dst);
}

Datum
omnisketch_in(PG_FUNCTION_ARGS)
{
	/*
	 * omnisketch stores the data in binary form and parsing text input is
	 * not needed, so disallow this.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "omnisketch")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}

Datum
omnisketch_out(PG_FUNCTION_ARGS)
{
	return byteaout(fcinfo);
}

Datum
omnisketch_recv(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "omnisketch")));

	PG_RETURN_VOID();			/* keep compiler quiet */
}

Datum
omnisketch_send(PG_FUNCTION_ARGS)
{
	return byteasend(fcinfo);
}

Datum
omnisketch_count(PG_FUNCTION_ARGS)
{
	omnisketch_t *sketch = PG_GETARG_OMNISKETCH(0);

	PG_RETURN_INT64(sketch->count);
}

/*
 * calculate intersection of an item list passed as the first argument, and
 * a new item list. This relies on the fact that the existing list can only
 * ever shrink, and both lists being sorted.
 */
static void
intersect_items(item_list_t *items, int n, int32 *tmp)
{
	int		i = 0,
			j = 0,
			k = 0;

	/*
	 * Walk the lists - both are sorted by hash of the item, so advance the
	 * list with lower hash value.
	 */
	while ((i < items->nitems) && (j < n))
	{
		uint32	h1,
				h2;

		if (items->items[i] == tmp[j])
		{
			items->items[k] = items->items[i];
			i++;
			j++;
			k++;
			continue;
		}

		h1 = SKETCH_HASH(items->items[i], SKETCH_SEED);
		h2 = SKETCH_HASH(tmp[j], SKETCH_SEED);

		/*
		 * FIXME compare the item too, not just hash, there may be collisions
		 * (even if not very likely)
		 */
		if (h1 < h2)
			i++;
		else
			j++;
	}

	items->nitems = k;
}

/*
 * Calculate the estimate using the algorithm from the paper. Visit all the
 * buckets matching the equality conditions, build the intersection of item
 * lists, etc.
 *
 * FIXME add info about function arguments
 */
static item_list_t *
omnisketch_column_estimate(omnisketch_t *sketch, int column, int32 hash,
						   int64 *maxcnt, item_list_t *items)
{
	for (int i = 0; i < sketch->sketchHeight; i++)
	{
		bucket_t   *bucket;
		int32	   *sample;

		uint32	h = (uint32) SKETCH_HASH(hash, i);
		int		j = (h % sketch->sketchWidth);

		bucket = SKETCH_BUCKET(sketch, column, i, j);
		sample = SKETCH_SAMPLE(sketch, column, i, j);

		if (bucket->totalCount > *maxcnt)
			*maxcnt = bucket->totalCount;

		/*
		 * First time through, just keep the list, otherwise merge the sample
		 * into the existing list. This is more efficient than bms_intersect
		 * (which also has to build the bitmap first).
		 */
		if (items == NULL)
		{
			items = palloc(offsetof(item_list_t, items) + sizeof(int32) * bucket->sampleCount);
			items->nitems = bucket->sampleCount;
			memcpy(items->items, sample, sizeof(int32) * items->nitems);
		}
		else
		{
			intersect_items(items, bucket->sampleCount, sample);
		}
	}

	return items;
}

Datum
omnisketch_estimate(PG_FUNCTION_ARGS)
{
	omnisketch_t *sketch = NULL;
	HeapTupleHeader record = PG_GETARG_HEAPTUPLEHEADER(1);

	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupdesc;
	HeapTupleData tuple;
	int			ncolumns;
	TypeCacheEntry **my_extra;
	Datum	   *values;
	bool	   *nulls;
	uint32		element_hash;
	int64		maxcnt = 0;
	item_list_t  *items = NULL;
	double		est;

	/* Build temporary HeapTuple control structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(record);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = record;

	tupType = HeapTupleHeaderGetTypeId(record);
	tupTypmod = HeapTupleHeaderGetTypMod(record);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
	ncolumns = tupdesc->natts;

	/* make sure to have a sketch */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	sketch = PG_GETARG_OMNISKETCH(0);

	AssertCheckSketch(sketch);

	if (sketch->numSketches != ncolumns)
		elog(ERROR, "number of record attributes mismatches sketch (%d != %d)",
			 ncolumns, sketch->numSketches);

	/* cache type info */
	my_extra = (TypeCacheEntry **) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL)
	{
		my_extra =
			MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt,
								   sizeof(TypeCacheEntry *) * ncolumns);
		fcinfo->flinfo->fn_extra = my_extra;
	}

	/* Break down the tuple into fields */
	values = (Datum *) palloc(ncolumns * sizeof(Datum));
	nulls = (bool *) palloc(ncolumns * sizeof(bool));
	heap_deform_tuple(&tuple, tupdesc, values, nulls);

	for (int i = 0; i < ncolumns; i++)
	{
		Form_pg_attribute att;
		TypeCacheEntry *typentry;

		att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;

		/*
		 * Lookup the hash function if not done already
		 */
		typentry = my_extra[i];
		if (typentry == NULL ||
			typentry->type_id != att->atttypid)
		{
			typentry = lookup_type_cache(att->atttypid,
										 TYPECACHE_HASH_EXTENDED_PROC_FINFO);
			if (!OidIsValid(typentry->hash_extended_proc_finfo.fn_oid))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("could not identify an extended hash function for type %s",
								format_type_be(typentry->type_id))));
			my_extra[i] = typentry;
		}

		/* Compute hash of element */
		if (nulls[i])
		{
			/* XXX Is it good enough to use 0 for NULL? */
			element_hash = 0;
		}
		else
		{
			LOCAL_FCINFO(locfcinfo, 2);

			InitFunctionCallInfoData(*locfcinfo, &typentry->hash_extended_proc_finfo, 2,
									 att->attcollation, NULL, NULL);
			locfcinfo->args[0].value = values[i];
			locfcinfo->args[0].isnull = false;
			locfcinfo->args[1].value = Int64GetDatum(0);
			locfcinfo->args[0].isnull = false;
			element_hash = DatumGetUInt64(FunctionCallInvoke(locfcinfo));

			/* We don't expect hash support functions to return null */
			Assert(!locfcinfo->isnull);
		}

		items = omnisketch_column_estimate(sketch, i, element_hash, &maxcnt, items);
	}

	pfree(values);
	pfree(nulls);

	ReleaseTupleDesc(tupdesc);

	/* Avoid leaking memory when handed toasted input. */
	// PG_FREE_IF_COPY(record, 0);

	est = maxcnt / sketch->sampleSize * items->nitems;

	pfree(items);

	PG_RETURN_INT64(est);
}

/*
 * text representation of the sketch
 */
Datum
omnisketch_text(PG_FUNCTION_ARGS)
{
	StringInfoData	str;
	omnisketch_t   *sketch = PG_GETARG_OMNISKETCH(0);

	AssertCheckSketch(sketch);

	initStringInfo(&str);

	appendStringInfo(&str, "sketches: %d width: %d height: %d sample: %d item: %d count: %d\n",
				sketch->numSketches, sketch->sketchWidth, sketch->sketchHeight,
				sketch->sampleSize, sketch->itemSize, sketch->count);

	/* dump the buckets */
	for (int i = 0; i < sketch->numSketches; i++)
	{
		if (i > 0)
			appendStringInfo(&str, ",\n");

		appendStringInfo(&str, "%d => {", i);

		for (int j = 0; j < sketch->sketchHeight; j++)
		{
			if (j > 0)
				appendStringInfo(&str, ",\n");

			appendStringInfoString(&str, "{");
			for (int k = 0; k < sketch->sketchWidth; k++)
			{
				bucket_t *bucket = SKETCH_BUCKET(sketch, i, j, k);
				if (k > 0)
					appendStringInfo(&str, ", ");
				appendStringInfo(&str, "(%d, %d) => (%d, %d)\n", j, k,
								 bucket->totalCount, bucket->sampleCount);
			}
			appendStringInfoString(&str, "}");
		}

		appendStringInfoString(&str, "}");
	}

	/* dump the samples */
	for (int i = 0; i < sketch->numSketches; i++)
	{
		if (i > 0)
			appendStringInfo(&str, ",\n");

		appendStringInfo(&str, "%d => {", i);

		for (int j = 0; j < sketch->sketchHeight; j++)
		{
			if (j > 0)
				appendStringInfo(&str, ",\n");

			appendStringInfoString(&str, "{");
			for (int k = 0; k < sketch->sketchWidth; k++)
			{
				bucket_t *bucket = SKETCH_BUCKET(sketch, i, j, k);
				int *sample = SKETCH_SAMPLE(sketch, i, j, k);

				AssertCheckBucket(sketch, bucket, sample);

				appendStringInfo(&str, "(%d, %d) => [", j, k);
				for (int l = 0; l < bucket->sampleCount; l++)
				{
					if (l > 0)
						appendStringInfo(&str, ", ");
					appendStringInfo(&str, "%d", sample[l]);
				}
				appendStringInfo(&str, "]\n");
			}
			appendStringInfoString(&str, "}");
		}

		appendStringInfoString(&str, "}");
	}

	PG_RETURN_TEXT_P(cstring_to_text(str.data));
}

/*
 * json representation of the sketch
 */
Datum
omnisketch_json(PG_FUNCTION_ARGS)
{
	StringInfoData	str;
	omnisketch_t   *sketch = PG_GETARG_OMNISKETCH(0);

	AssertCheckSketch(sketch);

	initStringInfo(&str);

	appendStringInfo(&str, "{\"sketches\": %d, \"width\": %d, \"height\": %d, \"sample\": %d, \"item\": %d, \"count\": %d, \"sketches\": [",
				sketch->numSketches, sketch->sketchWidth, sketch->sketchHeight,
				sketch->sampleSize, sketch->itemSize, sketch->count);

	/* dump the buckets, with samples */
	for (int i = 0; i < sketch->numSketches; i++)
	{
		if (i > 0)
			appendStringInfo(&str, ", ");

		appendStringInfoString(&str, "{\"buckets\": [");

		for (int j = 0; j < sketch->sketchHeight; j++)
		{
			if (j > 0)
				appendStringInfo(&str, ", ");

			for (int k = 0; k < sketch->sketchWidth; k++)
			{
				bucket_t *bucket = SKETCH_BUCKET(sketch, i, j, k);
				int *sample = SKETCH_SAMPLE(sketch, i, j, k);

				AssertCheckBucket(sketch, bucket, sample);

				if (k > 0)
					appendStringInfo(&str, ", ");

				appendStringInfo(&str,
								 "{\"i\": %d, \"j\": %d, \"total\": %d, \"sample\": %d, \"items\": [",
								 j, k, bucket->totalCount, bucket->sampleCount);

				for (int l = 0; l < bucket->sampleCount; l++)
				{
					if (l > 0)
						appendStringInfo(&str, ", ");
					appendStringInfo(&str, "%d", sample[l]);
				}

				appendStringInfoString(&str, "]}");
			}
		}

		appendStringInfoString(&str, "]}");
	}

	appendStringInfoString(&str, "]}");

	PG_RETURN_TEXT_P(cstring_to_text(str.data));
}
