CREATE TYPE omnisketch;

CREATE OR REPLACE FUNCTION omnisketch_in(cstring)
    RETURNS omnisketch
    AS 'omnisketch', 'omnisketch_in'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION omnisketch_out(omnisketch)
    RETURNS cstring
    AS 'omnisketch', 'omnisketch_out'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION omnisketch_send(omnisketch)
    RETURNS bytea
    AS 'omnisketch', 'omnisketch_send'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION omnisketch_recv(internal)
    RETURNS omnisketch
    AS 'omnisketch', 'omnisketch_recv'
    LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE omnisketch (
    INPUT = omnisketch_in,
    OUTPUT = omnisketch_out,
    RECEIVE = omnisketch_recv,
    SEND = omnisketch_send,
    INTERNALLENGTH = variable,
    STORAGE = external
);


CREATE OR REPLACE FUNCTION omnisketch_combine(omnisketch, omnisketch)
    RETURNS omnisketch
    AS 'omnisketch', 'omnisketch_combine'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION omnisketch_add(omnisketch, double precision, double precision, record)
    RETURNS omnisketch
    AS 'omnisketch', 'omnisketch_add'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION omnisketch_finalize(omnisketch)
    RETURNS omnisketch
    AS 'omnisketch', 'omnisketch_finalize'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE omnisketch(double precision, double precision, record) (
    SFUNC = omnisketch_add,
    STYPE = omnisketch,
    FINALFUNC = omnisketch_finalize,
    COMBINEFUNC = omnisketch_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE omnisketch(omnisketch) (
    SFUNC = omnisketch_combine,
    STYPE = omnisketch,
    FINALFUNC = omnisketch_finalize,
    COMBINEFUNC = omnisketch_combine,
    PARALLEL = SAFE
);

CREATE OR REPLACE FUNCTION omnisketch_count(omnisketch)
    RETURNS bigint
    AS 'omnisketch', 'omnisketch_count'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION omnisketch_estimate(omnisketch, record)
    RETURNS bigint
    AS 'omnisketch', 'omnisketch_estimate'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION omnisketch_text(omnisketch)
    RETURNS text
    AS 'omnisketch', 'omnisketch_text'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION omnisketch_json(omnisketch)
    RETURNS json
    AS 'omnisketch', 'omnisketch_json'
    LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (omnisketch AS text) WITH FUNCTION omnisketch_text(omnisketch);
CREATE CAST (omnisketch AS json) WITH FUNCTION omnisketch_json(omnisketch);
