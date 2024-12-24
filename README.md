# OmniSketch extension

[![make installcheck](https://github.com/tvondra/omnisketch/actions/workflows/ci.yml/badge.svg)](https://github.com/tvondra/omnisketch/actions/workflows/ci.yml)
[![PGXN version](https://badge.fury.io/pg/omnisketch.svg)](https://badge.fury.io/pg/omnisketch)

This PostgreSQL extension implements OmniSketch, a data structure for on-line
aggregation of data into approximate sketch, and answering queries.
The algorithm is also very friendly to parallel programs.

The OmniSketch data structure was introduced by this paper:

* OmniSketch: Efficient Multi-Dimensional High-Velocity Stream Analytics
  with Arbitrary Predicates; Wieger R. Punter, Odysseas Papapetrou, Minos
  Garofalakis; Proc. {VLDB} Endow. Volume 17, No. 3, p 319-331, 2023
  [PDF](https://www.vldb.org/pvldb/vol17/p319-punter.pdf)
  [PVLDB](http://vldb.org/pvldb/volumes/17/paper/OmniSketch%3A%20Efficient%20Multi-Dimensional%20High-Velocity%20Stream%20Analytics%20with%20Arbitrary%20Predicates)


## Basic usage

Precalculate a sketch from data, use it to estimate counts for predicates

```
-- Create a table with columns "a" and "b", with 100 distinct values, and
-- both columns correlated (a=b).
CREATE TABLE data (id INT, a INT, b INT);

INSERT INTO data SELECT i, mod(i,100), mod(i,100)
  FROM generate_series(1,1000000) s(i);

-- Pre-calculate sketches on deterministic partitions of the data set.
CREATE TABLE sketches AS
SELECT mod(id,10) AS p, omnisketch(0.01, 0.01, (a, b)) AS s
  FROM data GROUP BY mod(id,10);

-- Use the sketches to estimate condition (a = 9 AND b = 10).
SELECT omnisketch_estimate(omnisketch(s), (9, 10)) FROM sketches;

-- Use the sketches to estimate condition (a = 10 AND b = 10).
SELECT omnisketch_estimate(omnisketch(s), (10, 10)) FROM sketches;
```


## Functions

### `omnisketch(epsilon, delta, record)`

Calculate a sketch for values in the record, each record identified by ID
(which generated automatically). The `epsilon` and `delta` parameters
specify desired accuracy of the estimates (see the paper for meaning).
The lower the values, the more accurate (and larger) the sketch is going
to be.

#### Synopsis

```
SELECT omnisketch(0.01, 0.01, (a, b)) FROM data
```

#### Parameters

- `epsilon` - accuracy (relative to total records added), range `[0,1]`
- `delta` - accuracy, range `[0,1]`
- `record` - values to add to the sketch


### `omnisketch(sketch)`

Combine sketches into a new sketch. The sketches have to be compatible,
i.e. same accuracy parameters (which implies same structure).

#### Synopsis

```
SELECT omnisketch(s) FROM sketches
```

#### Parameters

- `sketch` - sketch created by `omnisketch(epsilon, delta, ...)`


### `omnisketch_count(omnisketch)`

Returns the total number of records added to the sketch so far.

#### Synopsis

```
SELECT omnisketch_count(s) FROM sketches
```

#### Parameters

- `sketch` - sketch created by `omnisketch(epsilon, delta, ...)`


### `omnisketch_estimate(omnisketch, record)`

Returns an estimate for number of records matching the predicates from
record (equality).

#### Synopsis

```
SELECT omnisketch_estimate(s, (a,b)) FROM sketches
```

#### Parameters

- `sketch` - sketch created by `omnisketch(epsilon, delta, ...)`


## Notes

This is an early experimental extension. Not only does it likely have
various issues, it may also change in unexpected and incompatible ways.
Don't use it for anything serious.

* The IDs are generated automatically by combining a (random) per-sketch
seed, with the sequence for each value added to the sketch. This allows
combining sketches (using just the sequence would cause duplicates,
e.g. with parallel queries). There's still some more work needed to deal
with duplicates (even if very unlikely). It can still happen, in which
case it can cause `ABORT` in assert, or affect estimates.

* The paper suggests to size the samples based on memory budget, but the
extension doesn't do that - at least not yet. Instead, it caps the
sample size to 1024, and that's it. But a memory budget is doable, and
it should also enforce the maximum 1GB allocation size.

* The paper also describes how to support range queries by building
sketches on dyadic ranges. That's not implemented yet.


## License

This software is distributed under the terms of PostgreSQL license.
See LICENSE or http://www.opensource.org/licenses/bsd-license.php for
more details.

