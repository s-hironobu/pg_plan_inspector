# Results

The results of the [benchmarks](README.md).

They are observations under pgpi's implementation and conditions. They do not rank the methods. They show how the framework supports research and improvement of such methods.

The tools for further analysis are in [client/tools](../client/tools/README.md). [Analysis](ANALYSIS.md) shows how to use them on these results, with worked examples.

## Environment

| | |
| --- | --- |
| Machine | Vagrant + VirtualBox on macOS; arm64, 2 CPUs, 8 GB memory, 60 GB storage |
| OS | Ubuntu 24.04.2 LTS (Linux 6.8.0-53-generic) |
| PostgreSQL | `shared_buffers = 2048MB`, `work_mem = 100MB`; the benchmarks set `random_page_cost = 1.1`, `max_parallel_workers_per_gather = 0`, `temp_file_limit = 3GB` |
| litecard | trees by ydf (Yggdrasil Decision Forests), compiled for arm64 Linux, as the paper uses it ([how](https://www.interdb.jp/blog/pgsql/ydf_on_arm64_linux/)) |

## How to read the results

- **vs pg** is the total execution time divided by PostgreSQL's in the same run. The total is the sum over the queries of the median of 3 runs. Planning time is not included. A few long queries can move a total. `per_query.tsv` shows them.
- **A and B** separate what the training covered from what it did not. A method that does well on A and not on B has learned the training distribution.
- **changed** tells how much a method intervened. With few changed plans, the time cannot move much.
- **q-error** is `max(est/actual, actual/est)`. It is computed on each method's own plans, on the estimates the planner used. The nodes differ from method to method. So it compares how well each method knew the plans it led to, not the same set of questions. A low q-error does not imply fast plans. A single large underestimate in the wrong place (a nested loop over a large input) can cost more than many small errors.

> The results come from pgpi's implementations of the methods, under pgpi's conditions (training only on executed plan nodes, shortened training; see [the estimators](../client/pgpi/estimator_plugins/README.md)), on one small machine.

## stats_ceb

```
$ cat stats_ceb/report.txt
== 1. STATS data (schema stats)
== 2. capture train.sql (544 queries) with PostgreSQL alone, collect
   90s; dataset: {'plans': 544, 'nodes': 4950, 'requests': 0, 'stats': 8}
== 3. fit feedback, matcomp, hres, litecard, nngp, grasp
   feedback                              0.9s
   matcomp                              13.1s
   hres                                  0.3s
   litecard                              1.5s
   nngp                                  0.7s
   grasp:epochs=100,restarts=1        1765.9s
== 4. run test.sql (89 queries: 57 A, 32 B) x 7 x 3 run(s), timeout 30s
   run 1/3 done (509s)
   run 2/3 done (922s)
   run 3/3 done (1335s)
== 5. results
   execution time: sum over the queries of the median of 3 run(s), ms; 'changed' = queries whose plan differs from PostgreSQL's; 'timeouts' = runs canceled at 30s or failed (counted as 30s)
   estimator         A ms  vs pg      B ms  vs pg    all ms  vs pg  changed timeouts  q-error median      p90
   hres             27378   0.94     15038   0.97     42417   0.95       22        0            1.32     18.4
   postgres         29241   1.00     15571   1.00     44812   1.00        0        0            1.25     15.9
   nngp             24180   0.83     21771   1.40     45951   1.03       73        0            2.06     30.0
   feedback         31362   1.07     15899   1.02     47261   1.05       35        0            1.62     28.2
   litecard         35290   1.21     15363   0.99     50653   1.13       49        0            1.50     66.6
   matcomp          41618   1.42     18824   1.21     60443   1.35       66        0            2.17    122.0
   grasp            58031   1.98     54198   3.48    112229   2.50       83        3            5.54   2422.7
```

On stats_ceb, PostgreSQL's own estimates are already close (q-error median 1.25, p90 15.9). No method's q-error is clearly below PostgreSQL's.

- hres changes the fewest plans (22) and is the only total below PostgreSQL's, by 5%.
- nngp is faster on A (0.83) and slower on B (1.40), the join graphs it was not trained on.
- litecard is the other way round (1.21 on A, 0.99 on B).
- matcomp and grasp change the most plans and are slower. grasp has the largest errors (p90 2423), the longest total (2.50) and the only timeouts.

## job_light

```
$ cat job_light/report.txt
== 1. IMDb data (schema imdb, from benchmark/lib/imdb.py)
   schema imdb: the 6 tables of JOB-light, about 62,107,384 rows
== 2. capture train.sql (630 queries) with PostgreSQL alone, collect
   1304s; dataset: {'plans': 630, 'nodes': 3233, 'requests': 0, 'stats': 6}
== 3. fit feedback, matcomp, hres, litecard, nngp, grasp
   feedback                              1.5s
   matcomp                              13.5s
   hres                                  0.2s
   litecard                              0.6s
   nngp                                  0.2s
   grasp:epochs=100,restarts=1         139.9s
== 4. run test.sql (70 queries: 35 A, 35 B) x 7 x 3 run(s), timeout 60s
   run 1/3 done (3552s)
   run 2/3 done (7023s)
   run 3/3 done (10517s)
== 5. results
   execution time: sum over the queries of the median of 3 run(s), ms; 'changed' = queries whose plan differs from PostgreSQL's; 'timeouts' = runs canceled at 60s or failed (counted as 60s)
   estimator         A ms  vs pg      B ms  vs pg    all ms  vs pg  changed timeouts  q-error median      p90
   hres            126972   1.03    334985   0.99    461958   1.00       13        6            1.28      9.8
   postgres        123467   1.00    339537   1.00    463004   1.00        0        6            1.31     12.5
   matcomp         125444   1.02    348439   1.03    473883   1.02       23        6            1.25      9.0
   feedback        125235   1.01    361094   1.06    486329   1.05       20        6            1.39     12.6
   litecard        129044   1.05    358103   1.05    487148   1.05       44        6            1.49     25.0
   nngp            117973   0.96    416607   1.23    534581   1.15       47        6            2.96     61.0
   grasp           186682   1.51    394301   1.16    580983   1.25       45        6            1.74     24.0
```

On job_light, PostgreSQL's estimates are already close (q-error median 1.31, p90 12.5). The methods change few plans (13 to 47 of 70).

- hres, matcomp, feedback and litecard are within 5% of PostgreSQL's total.
- nngp is faster on A (0.96) and slower on B (1.23).
- grasp is slower, most of all on A (1.51).

The 6 timeouts are the same in every line: queries that take more than 60 s whatever the estimator, each counted as 60 s.

## ceb_1a

```
$ cat ceb_1a/report.txt
== 1. IMDb data (schema imdb, from benchmark/lib/imdb.py)
   schema imdb: the 7 tables of CEB 1a, about 57,835,996 rows
== 2. capture train.sql (500 queries) with PostgreSQL alone, collect
   1541s; dataset: {'plans': 500, 'nodes': 9826, 'requests': 0, 'stats': 7}
== 3. fit feedback, matcomp, hres, litecard, nngp, grasp
   feedback                              1.6s
   matcomp                             141.1s
   hres                                  0.9s
   litecard                              7.2s
   nngp                                  6.1s
   grasp:epochs=100,restarts=1         420.0s
== 4. run test.sql (160 queries: 100 A, 60 B) x 7 x 3 run(s), timeout 60s
   run 1/3 done (4166s)
   run 2/3 done (7919s)
   run 3/3 done (11675s)
== 5. results
   execution time: sum over the queries of the median of 3 run(s), ms; 'changed' = queries whose plan differs from PostgreSQL's; 'timeouts' = runs canceled at 60s or failed (counted as 60s)
   estimator         A ms  vs pg      B ms  vs pg    all ms  vs pg  changed timeouts  q-error median      p90
   hres            242556   0.87    112885   0.96    355441   0.89      132        0            1.84     54.9
   feedback        245014   0.88    112659   0.96    357674   0.90      132        0            1.72     28.8
   postgres        279479   1.00    117840   1.00    397319   1.00        0        0           10.39   1574.5
   nngp            274543   0.98    159028   1.35    433571   1.09      160        0            2.00     25.9
   litecard        339185   1.21    141306   1.20    480491   1.21      160        0            1.84    315.5
   matcomp         341881   1.22    161066   1.37    502946   1.27      160        0            1.55     26.1
   grasp           493169   1.76    671227   5.70   1164396   2.93      160        3            1.75     27.9
```

On ceb_1a, PostgreSQL's estimates are far off (q-error median 10.4, p90 1575). Every method's median is far below it, while the totals range from 0.89 to 2.93. Here, a lower q-error does not mean a shorter total.

- hres and feedback are 10 to 11% faster than PostgreSQL in total: 12 to 13% on A and 4% on B.
- nngp has one of the smallest p90 (25.9). It is about the same on A (0.98) and slower on B (1.35).
- litecard is 1.21 times slower.
- matcomp and grasp have medians and p90 comparable to the others'. They are 1.27 and 2.93 times slower, grasp most of all on B (5.70). grasp has 3 timeouts.

[Analysis](ANALYSIS.md) examines why litecard ([case](ANALYSIS.md#case-litecard-on-ceb_1a)) and grasp ([case](ANALYSIS.md#case-grasp-on-ceb_1a)) are slower.

## The q-error distributions

The reports give the median and p90 of the q-error. `client/tools/qerror_runs.py` gives the whole distribution of the same values: the estimates the planner used, on each label's own plans.

```
$ python3 tools/qerror_runs.py --data ../benchmark/data/stats_ceb/ds/
label                            point           n   gmean  median       p90       p99
stats-feedback                   all          1500    3.33    1.62      28.2    2323.2
stats-grasp                      all          1446   18.97    5.54    2422.7  318561.0
stats-hres                       all          1455    2.74    1.32      18.4    1443.9
stats-litecard                   all          1437    4.04    1.50      66.6  595820.0
stats-matcomp                    all          1530    5.59    2.17     122.0   21091.0
stats-nngp                       all          1578    3.82    2.06      30.0    1931.1
stats-postgres                   all          1476    2.60    1.25      15.9    1739.9
stats-train                      all          2870    4.66    1.75     107.7   68316.0

$ python3 tools/qerror_runs.py --data ../benchmark/data/job_light/ds/
label                            point           n   gmean  median       p90       p99
jl-feedback                      all          1212    2.40    1.39      12.6     847.4
jl-grasp                         all          1076    3.30    1.74      24.0    4929.5
jl-hres                          all          1195    2.08    1.28       9.8     694.7
jl-litecard                      all          1099    2.78    1.49      25.0     962.6
jl-matcomp                       all          1185    2.06    1.25       9.0     308.4
jl-nngp                          all          1218    5.01    2.96      61.0    2512.3
jl-postgres                      all          1206    2.30    1.31      12.5     694.7
jl-train                         all          2035    1.82    1.19       5.1      53.0

$ python3 tools/qerror_runs.py --data ../benchmark/data/ceb_1a/ds/
label                            point           n   gmean  median       p90       p99
ceb-feedback                     all          7410    3.26    1.72      28.8     583.1
ceb-grasp                        all          7756    3.07    1.75      27.9     307.5
ceb-hres                         all          7383    3.93    1.84      54.9    1067.3
ceb-litecard                     all          7588    6.89    1.84     315.5   47318.8
ceb-matcomp                      all          7428    3.11    1.55      26.1     834.0
ceb-nngp                         all          7392    3.36    2.00      25.9     402.4
ceb-postgres                     all          7227   19.81   10.39    1574.5   25082.0
ceb-train                        all          7626   14.10    6.96     969.0   31916.0
```

Reading them:

- The medians and p90 are those of the reports. The geometric mean and p99 show the tails. Two estimators with the same median can differ by orders of magnitude at p99.
  - litecard's p99 (6 x 10^5 on stats_ceb, 4.7 x 10^4 on ceb_1a) comes from its bias correction. See [Case: litecard on ceb_1a](ANALYSIS.md#case-litecard-on-ceb_1a).
- `n` differs between labels, because each estimator leads to plans of its own. `*-train` and `*-postgres` show PostgreSQL's own errors on the training and the test queries.

The whole distribution does not order the estimators by execution time either. On ceb_1a, grasp has the smallest p99 (308), the smallest geometric mean and the longest total. hres and feedback have larger tails and the shortest totals.

What matters more than the average size of the errors is where an estimator is wrong, and where it abstains. When it abstains, PostgreSQL's estimate stays, and the plan is chosen with it.
