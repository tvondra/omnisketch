\set ECHO all

SET max_parallel_workers_per_gather = 4;
SET max_parallel_maintenance_workers = 4;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;

CREATE TABLE d (id int, a int, b int, c int, d int, e int, f int);

INSERT INTO d
SELECT i, mod(i,100), mod(i,100), mod(i,100), mod(i,1000), mod(i,1000), mod(i,1000)
  FROM generate_series(1,100000) s(i);

ANALYZE d;

CREATE TABLE t AS
SELECT mod(id,10), omnisketch(0.01, 0.01, (a, b)) AS s
FROM d GROUP BY mod(id,10);

SELECT omnisketch_count(omnisketch(t.s)) FROM t;

SELECT (SELECT omnisketch_estimate(omnisketch(t.s), (i, i)) FROM t) BETWEEN 500 AND 1500 AS e FROM generate_series(1,10) s(i);

SELECT (SELECT omnisketch_estimate(omnisketch(t.s), (i, i+1)) FROM t) < 500 AS e FROM generate_series(1,10) s(i);


INSERT INTO d
SELECT i, mod(i,100), mod(i,100), mod(i,100), mod(i,1000), mod(i,1000), mod(i,1000)
  FROM generate_series(100001,1000000) s(i);

DROP TABLE t;

CREATE TABLE t AS
SELECT mod(id,10), omnisketch(0.01, 0.01, (a, b)) AS s
FROM d GROUP BY mod(id,10);

SELECT omnisketch_count(omnisketch(t.s)) FROM t;

SELECT (SELECT omnisketch_estimate(omnisketch(t.s), (i, i)) FROM t) BETWEEN 5000 AND 15000 AS e FROM generate_series(1,10) s(i);

SELECT (SELECT omnisketch_estimate(omnisketch(t.s), (i, i+1)) FROM t) < 500 AS e FROM generate_series(1,10) s(i);

DROP TABLE t;
DROP TABLE d;
