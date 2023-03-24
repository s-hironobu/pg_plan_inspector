Demonstration
=============

## Contents

1. [Finding functional dependency](#1-finding-functional-dependency)  
2. [Adaptive work memory expansion](#2-adaptive-work-memory-expansion)


## 1. Finding functional dependency

I demonstrate how to find a functional dependency between the attributes of the table using an extreme example.

Also watch the movie: [pg_plan_inspector_analyze_01.mp4](https://user-images.githubusercontent.com/7246769/126769418-454362aa-cf0e-4d47-bbf3-a3e60ebffee1.mp4)



https://user-images.githubusercontent.com/7246769/126769418-454362aa-cf0e-4d47-bbf3-a3e60ebffee1.mp4



Let us create two tables as shown below.
As observed, there is a strong dependency between the columns in each table.

```
testdb=# SHOW pg_query_plan.log_min_duration;
 pg_query_plan.log_min_duration
--------------------------------
 3
(1 row)

testdb=# CREATE UNLOGGED TABLE t1 (a1 int, b1 int);
CREATE TABLE
testdb=# CREATE UNLOGGED TABLE t2 (a2 int, b2 int);
CREATE TABLE
testdb=# INSERT INTO t1 SELECT i/100, i/500 FROM generate_series(1,5000000) s(i);
INSERT 0 5000000
testdb=# INSERT INTO t2 SELECT i/100, i/500 FROM generate_series(1,5000000) s(i);
INSERT 0 5000000
testdb=# ANALYZE;
ANALYZE
testdb=# \timing
Timing is on.
```

Then, issue the following SELECT command.
In my environment, the duration time of this command was 13.2 [sec].

```
testdb=# SELECT count(*) FROM t1, t2 WHERE (t2.a2 = 1) AND (t2.b2 = 0) AND (t1.b1 = t2.b2);
 count
-------
 49900
(1 row)

Time: 13245.965 ms (00:13.246)

testdb=# EXPLAIN SELECT count(*) FROM t1, t2 WHERE (t2.a2 = 1) AND (t2.b2 = 0) AND (t1.b1 = t2.b2);
                                    QUERY PLAN
-----------------------------------------------------------------------------------
 Aggregate  (cost=103595.90..103595.91 rows=1 width=8)
   ->  Nested Loop  (cost=2000.00..103594.66 rows=499 width=0)
         ->  Gather  (cost=1000.00..54374.10 rows=1 width=4)
               Workers Planned: 2
               ->  Parallel Seq Scan on t2  (cost=0.00..53374.00 rows=1 width=4)
                     Filter: ((a2 = 1) AND (b2 = 0))
         ->  Gather  (cost=1000.00..49215.57 rows=499 width=4)
               Workers Planned: 2
               ->  Parallel Seq Scan on t1  (cost=0.00..48165.67 rows=208 width=4)
                     Filter: (b1 = 0)
(10 rows)
```

Next, we execute the repo_mgr.py command to get the data from the query_plan.log table.
Further, we execute the analyze.py command in the tools directory.

```
$ ./repo_mgr.py get --basedir test_repo/ server_1
Use test_repo/pgpi_repository:
Info: Connection established to 'server_1'.
Info: Getting query_plan.log table data.
Info: Connection closed.
Info: Grouping json formated plans.
Info: Calculating regression parameters.

cd tools/
$ ./analyze.py --basedir ../test_repo/ server_1
Info: Create 'es_server_1.dat'
Info: Create 'hist_server_1.dat'
```

As shown above, analyze.py created `es_server_1.dat` file, so show it.

```
$ cat es_server_1.dat
Candidate:
database=testdb
queryid=17259326547054606714
Condiftion:['((t2.a2 = 1) AND (t2.b2 = 0))']
query:SELECT count(*) FROM t1, t2 WHERE (t2.a2 = 1) AND (t2.b2 = 0) AND (t1.b1 = t2.b2);

Candidate:
database=testdb
queryid=17259326547054606714
Condiftion:['((t2.a2 = 1) AND (t2.b2 = 0))']
query:SELECT count(*) FROM t1, t2 WHERE (t2.a2 = 1) AND (t2.b2 = 0) AND (t1.b1 = t2.b2);
```

According to the file,
the analyze.py command shows candidates for which extended statistics should be set.
Thus, we will create extended statistics using the following CREATE STATISTICS command.

```
testdb=# CREATE STATISTICS es (dependencies) ON a2, b2 FROM t2;
CREATE STATISTICS
Time: 8.104 ms
testdb=# ANALYZE;
ANALYZE
Time: 1214.683 ms (00:01.215)
```

By the created statistics, the execution plan has been improved and this SELECT command can be completed within 0.29 [sec].

```
testdb=# SELECT count(*) FROM t1, t2 WHERE (t2.a2 = 1) AND (t2.b2 = 0) AND (t1.b1 = t2.b2);
 count
-------
 49900
(1 row)

Time: 287.878 ms

testdb=# EXPLAIN SELECT count(*) FROM t1, t2 WHERE (t2.a2 = 1) AND (t2.b2 = 0) AND (t1.b1 = t2.b2);
                                       QUERY PLAN
----------------------------------------------------------------------------------------
 Aggregate  (cost=104352.67..104352.68 rows=1 width=8)
   ->  Nested Loop  (cost=2000.00..104227.18 rows=50197 width=0)
         ->  Gather  (cost=1000.00..49215.37 rows=497 width=4)
               Workers Planned: 2
               ->  Parallel Seq Scan on t1  (cost=0.00..48165.67 rows=207 width=4)
                     Filter: (b1 = 0)
         ->  Materialize  (cost=1000.00..54384.60 rows=101 width=4)
               ->  Gather  (cost=1000.00..54384.10 rows=101 width=4)
                     Workers Planned: 2
                     ->  Parallel Seq Scan on t2  (cost=0.00..53374.00 rows=42 width=4)
                           Filter: ((a2 = 1) AND (b2 = 0))
(11 rows)
```

The analyze.py command shows that it can detect the pair of attributes of the table with functional dependency using a simple heuristics rule.
In this example, it has considerably reduced the query execution time.


This is an ad hoc, however, at least it shows that the functional dependencies can be detected using the executed plans.


#### Note
As you have already noticed, similar functionality can also be implemented using the auto_explain module, not the pg_query_plan module.


## 2. Adaptive work memory expansion

This feature is an experimental implementation,
and it temporarily expands the work_mem area during this query execution if the query has ever used temporary files.


Let us see an example.


At first, we execute the following query twice.


```
postgres=# SELECT id FROM tbl_big ORDER BY id DESC;
```

As shown the following result, The query used temporary files for sorting.


```
postgres=# select seqid, queryid, query, planid,plan from query_plan.log where queryid = '4294305104826516741';
-[ RECORD 1 ]-------------------------------------------------------------------------------------------------------
seqid   | 1381
queryid | 4294305104826516741
query   | SELECT id FROM tbl_big ORDER BY id DESC;
planid  | 6000439534193952791
plan    | Sort  (cost=127757.69..130257.70 rows=1000003 width=4) (actual rows=1000003 loops=1)                      +
        |   Output: id                                                                                              +
        |   Sort Key: tbl_big.id DESC                                                                               +
        |   Sort Method: external merge  Disk: 11768kB                                                              +
        |   ->  Seq Scan on public.tbl_big  (cost=0.00..14425.03 rows=1000003 width=4) (actual rows=1000003 loops=1)+
        |         Output: id                                                                                        +
        |
-[ RECORD 2 ]-------------------------------------------------------------------------------------------------------
seqid   | 1383
queryid | 4294305104826516741
query   | SELECT id FROM tbl_big ORDER BY id DESC;
planid  | 6000439534193952791
plan    | Sort  (cost=127757.69..130257.70 rows=1000003 width=4) (actual rows=1000003 loops=1)                      +
        |   Output: id                                                                                              +
        |   Sort Key: tbl_big.id DESC                                                                               +
        |   Sort Method: external merge  Disk: 11768kB                                                              +
        |   ->  Seq Scan on public.tbl_big  (cost=0.00..14425.03 rows=1000003 width=4) (actual rows=1000003 loops=1)+
        |         Output: id                                                                                        +
        |
```


We do repo_mgr.py with get and push commands for updating the query_plan.reg table.


```
$ ./repo_mgr.py get --basedir test_repo server_1
Use test_repo/pgpi_repository:
Info: Connection established to 'server_1'.
Info: Getting query_plan.log table data.
Info: Connection closed.
Info: Grouping json formated plans.
Info: Calculating regression parameters.
$ ./repo_mgr.py push --basedir test_repo server_1
Use test_repo/pgpi_repository:
```

After that, we execute the SEELCT command again.

```
postgres=# SELECT id FROM tbl_big ORDER BY id DESC;
```

At this time,
PostgreSQL temporarily expands the work_mem area during the query execution
because PostgreSQL recognizes that this query needs more work_mem by using the query_plan.reg.


See the following RECORD 3.
You can confirm that PostgreSQL used the work_mem instead of temporary files for sorting tuples.


```
postgres=# select seqid, queryid, query, planid,plan from query_plan.log where queryid = '4294305104826516741';
-[ RECORD 1 ]-------------------------------------------------------------------------------------------------------
seqid   | 1381
queryid | 4294305104826516741
query   | SELECT id FROM tbl_big ORDER BY id DESC;
planid  | 6000439534193952791
plan    | Sort  (cost=127757.69..130257.70 rows=1000003 width=4) (actual rows=1000003 loops=1)                      +
        |   Output: id                                                                                              +
        |   Sort Key: tbl_big.id DESC                                                                               +
        |   Sort Method: external merge  Disk: 11768kB                                                              +
        |   ->  Seq Scan on public.tbl_big  (cost=0.00..14425.03 rows=1000003 width=4) (actual rows=1000003 loops=1)+
        |         Output: id                                                                                        +
        |
-[ RECORD 2 ]-------------------------------------------------------------------------------------------------------
seqid   | 1383
queryid | 4294305104826516741
query   | SELECT id FROM tbl_big ORDER BY id DESC;
planid  | 6000439534193952791
plan    | Sort  (cost=127757.69..130257.70 rows=1000003 width=4) (actual rows=1000003 loops=1)                      +
        |   Output: id                                                                                              +
        |   Sort Key: tbl_big.id DESC                                                                               +
        |   Sort Method: external merge  Disk: 11768kB                                                              +
        |   ->  Seq Scan on public.tbl_big  (cost=0.00..14425.03 rows=1000003 width=4) (actual rows=1000003 loops=1)+
        |         Output: id                                                                                        +
        |
-[ RECORD 3 ]-------------------------------------------------------------------------------------------------------
seqid   | 1390
queryid | 4294305104826516741
query   | SELECT id FROM tbl_big ORDER BY id DESC;
planid  | 16216934002595222227
plan    | Sort  (cost=127757.69..130257.70 rows=1000003 width=4) (actual rows=1000003 loops=1)                      +
        |   Output: id                                                                                              +
        |   Sort Key: tbl_big.id DESC                                                                               +
        |   Sort Method: quicksort  Memory: 24577kB                                                                 +
        |   ->  Seq Scan on public.tbl_big  (cost=0.00..14425.03 rows=1000003 width=4) (actual rows=1000003 loops=1)+
        |         Output: id                                                                                        +
        |
```