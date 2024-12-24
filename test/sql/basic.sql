\set ECHO none

-- disable the notices for the create script (shell types etc.)
SET client_min_messages = 'WARNING';
\i omnisketch--1.0.0.sql
\i omnisketch--1.0.0--1.0.1.sql
SET client_min_messages = 'NOTICE';

\set ECHO all

SET max_parallel_workers_per_gather = 0;
SET max_parallel_maintenance_workers = 0;

CREATE TABLE d (id int, a int, b int, c int, d int, e int, f int);

INSERT INTO d
SELECT i, mod(i,100), mod(i,100), mod(i,100), mod(i,1000), mod(i,1000), mod(i,1000)
  FROM generate_series(1,100000) s(i);

ANALYZE d;

CREATE TABLE t AS
SELECT mod(id,10), omnisketch(0.01, 0.01, (a, b)) AS s
  FROM d GROUP BY mod(id,10);

SELECT omnisketch_count(omnisketch(t.s)) FROM t;

SELECT omnisketch_estimate(omnisketch(t.s), (1, 2, 3)) FROM t;

SELECT (SELECT omnisketch_estimate(omnisketch(t.s), (i, i)) FROM t) BETWEEN 500 AND 1500 AS e FROM generate_series(1,10) s(i);

SELECT (SELECT omnisketch_estimate(omnisketch(t.s), (i, i+1)) FROM t) < 500 AS e FROM generate_series(1,10) s(i);

DROP TABLE t;
DROP TABLE d;
