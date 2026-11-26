# pgpi demo

One command runs the Quick Start workload through every bundled estimation method, and compares them by execution time and q-error.

The data and workload are those of the Quick Start ([README 5](../README.md#5-quick-start)). The demo is the quickest way to see all the plugins side by side, on a small schema.

## 1. Data structure

```text
demo/
  shop.sql     Schema `shop`: customers (50k), products (5k), orders (500k), lineitems (1.5M rows)
               (region = segment, category = brand; 40% of orders are from region r0)
  train.sql    40 training queries: templates T1–T4, literals r0–r9, c0–c9
  test.sql     100 test queries: T1–T4 with unseen literals, and T5–T10 (unseen templates)
  run.py       Script to run the Quick Start across all plugins
```

A **template** is a query with its literals replaced by `?`. T3, the 4-way join of the Quick Start, is one template: `... WHERE c.region = ? AND c.segment = ? AND p.category = ? AND p.brand = ?`. A **literal** is one such constant, for example `r10` in `c.region = 'r10'`.

## 2. How the demo works

The `shop` schema is created on the first run (about one minute; `--load` recreates it). The pipeline then has three steps:

1. **Capture:** Runs `train.sql` with native PostgreSQL, and collects the execution plans into a training dataset.
2. **Fit:** Trains every plugin whose Python dependencies are installed on that dataset.
3. **Evaluate:** Runs `test.sql` with native PostgreSQL and with each plugin (`enforce`). The results are reported for two query groups:
   * **A:** Templates T1–T4 with unseen literals. The questions are the same, with new constants: the literal `r10` where training used `r0`–`r9` (in-distribution generalization).
   * **B:** Templates T5–T10, never seen during training. The questions are new: a date range, a `GROUP BY`, an `IN` list, an `EXISTS` (out-of-distribution generalization).

## 3. How to run

```sh
export DSN="host=localhost user=postgres dbname=testdb"   # Target database for pgpi.log_database
python3 demo/run.py --dsn "$DSN"              # Run all plugins (3 runs per query)
python3 demo/run.py --dsn "$DSN" --quick      # Fast mode (1 run per query, shorter training)
python3 demo/run.py --dsn "$DSN" --plugins feedback,hres
```

Output files are written to `/tmp/pgpi-demo/quickstart/` (configurable via `--out`):

- `report.txt`: execution time and q-error per method.
- `per_query.tsv`: every test query, every estimator.
- `ds/`: the collected dataset.

> **Note:** Existing rows in `pgpi.plans` and `pgpi.stats` are cleared before execution.

## 4. Example output

```text
$ python3 demo/run.py --dsn "$DSN" --quick
== 1. Quick Start data (schema shop, demo/shop.sql)
== 2. capture train.sql (40 queries) with PostgreSQL alone, collect
   7s; dataset: {'plans': 40, 'nodes': 280, 'requests': 0, 'stats': 4}
== 3. fit feedback, matcomp, hres, litecard, nngp, grasp
   feedback                              1.2s
   matcomp:restarts=1                    0.2s
   hres                                  0.0s
   litecard                              0.1s
   nngp                                  0.0s
   grasp:epochs=30,restarts=1            2.7s
== 4. run test.sql (100 queries: 40 A, 60 B) x 7 x 1 run(s), timeout 30s
   run 1/1 done (48s)
== 5. results
   execution time: sum over the queries of the median of 1 run(s), ms; 'changed' = queries whose plan differs from PostgreSQL's; 'timeouts' = runs canceled at 30s or failed (counted as 30s)
   estimator         A ms  vs pg      B ms  vs pg    all ms  vs pg  changed timeouts  q-error median      p90
   feedback           798   0.63      2404   0.62      3202   0.62       50        0            1.41     39.0
   grasp              813   0.64      2405   0.62      3218   0.63       60        0            1.69      8.4
   matcomp            762   0.60      2470   0.64      3233   0.63       50        0            1.40     45.1
   litecard           851   0.67      2464   0.64      3314   0.65       58        0            2.00     33.3
   hres               756   0.60      2666   0.69      3423   0.67       70        0            1.66      3.2
   postgres          1266   1.00      3857   1.00      5124   1.00        0        0           19.31     50.0
   nngp               754   0.60     15357   3.98     16111   3.14       50        0            1.68     27.8
   per query: /tmp/pgpi-demo/quickstart/per_query.tsv
```

### 4.1. Understanding the output

The step numbers are those of the `== N.` lines in the output.

* **Step 2 (`dataset`):** What the training runs collected. 40 plans with 280 nodes (a node is a subquery, with PostgreSQL's estimate and its actual row count), `0` live requests (PostgreSQL ran alone, so no estimator was asked), and 4 snapshots of table statistics.
* **Step 3 (Training):** The training time per plugin. The parameter suffixes show the reduced settings of `--quick` (`matcomp:restarts=1`, `grasp:epochs=30,restarts=1`).
* **Step 4 (Evaluation):** 100 test queries, run with native PostgreSQL and with the six plugins (`x 7`). One run per query under `--quick`, 3 by default.
* **Step 5 (Results table):** Sorted by total execution time (`all ms`). The columns:
  + **`A ms`, `B ms`, `all ms`:** The sum of the median execution times (milliseconds) of the queries in each group. **`vs pg`** is the ratio to native PostgreSQL: 0.63 means 37% less time, and lower is faster. The times cover *execution only*. Planning and estimator inference are excluded.
  + **`changed`:** The number of test queries (out of 100) whose chosen plan differs from native PostgreSQL's. In the example, feedback changed 50 plans. An estimator cannot improve execution time unless it changes the plan.
  + **`timeouts`:** Runs canceled at the 30s timeout, exceeding `temp_file_limit`, or canceled during planning. Each counts as 30s in the sums.
  + **`q-error median`, `p90`:** The median and the 90th percentile of the q-error across the plan nodes of each method's own test runs. The **q-error** is max(est/actual, actual/est): an estimate of 10 rows for an actual 1,000 gives 100, and a perfect estimate gives 1. Each method is scored on its own chosen plans, which vary across estimators.

> **Note:** Individual queries on this synthetic dataset run in tens of milliseconds. A single run (`--quick`) therefore leaves noticeable variance from run-to-run noise.

`run.py` shares the runner, the CLI options and the reporting format with the main benchmark suite (see [benchmark/README.md](../benchmark/README.md)). The suite evaluates methods on real-world workloads (`STATS-CEB`, `JOB-light` and `CEB-IMDb`).
