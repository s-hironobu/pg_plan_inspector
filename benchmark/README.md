# pgpi benchmarks

The benchmarks train every estimator on a workload, then compare them by the execution time of the plans they lead PostgreSQL to and by the q-error of the estimates the planner used.

Each benchmark is one command: `python3 benchmark/stats_ceb/run.py`. It loads or checks the data, trains the estimators, runs the test queries and prints a report. The estimators are described in [the estimators](../client/pgpi/estimator_plugins/README.md).

| Benchmark | Data | Queries | Split | Time (2-core VM) |
| --- | --- | --- | --- | --- |
| [`stats_ceb`](#stats_ceb) | STATS (Stack Exchange, 8 tables, about 1 M rows), downloaded once | train 544, test 89 | A: join graphs seen in training (57), B: held out (32) | about 1 hour |
| [`job_light`](#job_light) | IMDb of JOB, 6 tables, prepared by hand | train 630 (of 1941), test 70 | A: at most 3 tables (35), B: 4 or 5 tables (35) | 3 to 4 hours |
| [`ceb_1a`](#ceb_1a) | IMDb of JOB, 7 tables, prepared by hand | train 500, test 160 | A: same information kinds as training (100), B: other kinds (60) | 5 to 6 hours |

The times are for the plugins `feedback,litecard,nngp,grasp,matcomp,hres` with 3 runs per query. Most of the time goes to running the test queries. Training grasp takes up to half an hour. `--quick` takes about a third.

## Preparation: The IMDb data

The two benchmarks job_light and ceb_1a use 10 of the 21 tables of the IMDb data of JOB, and only the columns their queries use. A benchmark never downloads this data. (`stats_ceb` downloads its own data, so it needs no preparation.) `lib/imdb.py` prepares it in three steps. Each step can be run, and rerun, on its own. The files go to a directory outside pgpi (default `~/imdb-job`, or `$PGPI_IMDB_DIR`; `--dir` chooses another). `$DSN` is the connection string of [Usage](#usage):

```sh
python3 benchmark/lib/imdb.py download                  # imdb.tgz, about 1.2 GB (resumes if interrupted)
python3 benchmark/lib/imdb.py extract                   # keep the 10 tables (gzipped CSV), delete imdb.tgz
python3 benchmark/lib/imdb.py import --dsn "$DSN"       # every table whose file is there, one at a time, into schema imdb
python3 benchmark/lib/imdb.py status --dsn "$DSN"       # what is downloaded, extracted and imported
```

- `extract` reads the archive as a stream and writes only the 10 tables. The other 11 never reach the disk. If the archive was unpacked by hand, `extract` compresses the CSV files it needs and deletes the others.
- `import` loads one table at a time. `--table cast_info` imports just that one. `--remove` deletes each CSV file once its table is imported.
- Only the columns the queries use are stored: about 3 GB for the 6 tables of job_light, about 5 GB for all 10 (with indexes).
- Tables imported before ceb_1a was added lack columns it needs (`cast_info.person_id`, `movie_info.info`). `status` marks them, and `import --table cast_info --table movie_info` imports them again.
- The row counts are those of the full data set. The execution times are not: the rows are narrower than in a full load, so the times are not comparable with published JOB timings.
- The IMDb data is for personal and non-commercial use ([IMDb](https://www.imdb.com/interfaces/)). The JOB-light queries and the MSCN training queries are from [learnedcardinalities](https://github.com/andreaskipf/learnedcardinalities) (MIT license, `job_light/LICENSE-learnedcardinalities`).

## Layout

```
benchmark/
  README.md            this document
  stats_ceb/           run.py, train.sql (544 queries), test.sql (89 queries)
  job_light/           run.py, train.sql (1941 queries; the first 630 are used), test.sql (70 queries),
                       LICENSE-learnedcardinalities
  ceb_1a/              run.py, queries.txt (the names of the queries used: 500 training, 100 A, 60 B)
  samples/             the datasets and the outputs behind RESULTS.md and ANALYSIS.md (see samples/README.md)
  lib/                 the runner and the data
    bench.py             the steps and the report (also used by demo/run.py)
    runner.py            data loading, the observation log, sockets
    stats.py             downloads and loads the STATS data
    imdb.py              prepares the IMDb data by hand: download, extract, import
    ceb.py               downloads the CEB queries
  scripts/             runs with fixed settings (see [The scripts](#the-scripts-in-scripts))
  data/                the default output (not under version control)
```

## Usage

```sh
cd pgpi                       # the repository
export DSN="host=localhost user=postgres dbname=testdb"   # the database of pgpi.log_database

python3 benchmark/stats_ceb/run.py --dsn "$DSN"
python3 benchmark/job_light/run.py --dsn "$DSN" --plugins feedback,hres --quick
python3 benchmark/ceb_1a/run.py    --dsn "$DSN" --plugins litecard --out /somewhere/ceb_1a-litecard
```

Example: a short run with two plugins and one run per query (the environment is in [Results](RESULTS.md#environment)).

```
$ python3 benchmark/job_light/run.py --dsn "$DSN" --plugins feedback,hres --quick
== 1. IMDb data (schema imdb, from benchmark/lib/imdb.py)
   schema imdb: the 6 tables of JOB-light, about 62,107,384 rows
== 2. capture train.sql (210 queries) with PostgreSQL alone, collect
   100/210 (281s)
   200/210 (515s)
   533s; dataset: {'plans': 210, 'nodes': 1063, 'requests': 0, 'stats': 6}
== 3. fit feedback, hres
   feedback                              0.8s
   hres                                  0.1s
== 4. run test.sql (70 queries: 35 A, 35 B) x 3 x 1 run(s), timeout 60s
   run 1/1 done (1387s)
== 5. results
   execution time: sum over the queries of the median of 1 run(s), ms; 'changed' = queries whose plan differs from PostgreSQL's; 'timeouts' = runs canceled at 60s or failed (counted as 60s)
   estimator         A ms  vs pg      B ms  vs pg    all ms  vs pg  changed timeouts  q-error median      p90
   postgres        118129   1.00    335951   1.00    454080   1.00        0        2            1.31     11.8
   hres            117633   1.00    337865   1.01    455498   1.00       16        2            1.29     10.5
   feedback        114036   0.97    360985   1.07    475022   1.05       20        2            1.54     11.6
```

Before a run:

1. Confirm that the IMDb data is already imported ([Preparation](#preparation-the-imdb-data)). job_light needs 6 tables of IMDb.
2. Confirm that no other estimation server is running. If one is running, stop it.

The steps in the output:

1. The data is checked. job_light needs 6 tables of IMDb.
2. The training queries (210 with `--quick`) run with PostgreSQL alone. `dataset` is what was collected: plans, plan nodes (subqueries with PostgreSQL's estimate and the actual rows), live requests (none yet) and versions of table statistics.
3. Each plugin is fitted. Its training time is printed.
4. Each test query runs with each plugin and with PostgreSQL alone (`x 3`), `--reps` times (1 with `--quick`).
5. The report ([Output](#output), [How to read the results](RESULTS.md#how-to-read-the-results)).

The scripts add `client/` to the path, so pgpi needs no installation. The Python packages some plugins need must be installed: `torch` for grasp, `sqlglot` and `scikit-learn` (or `ydf`) for litecard. A plugin whose packages are missing is skipped.

| Option | Default | Description |
| --- | --- | --- |
| `--dsn` | `$PGPI_BENCH_DSN`, else `$DSN`, else libpq's defaults | The database of `pgpi.log_database` (the runs are read back from `pgpi.plans` there) |
| `--out` | `benchmark/data/<benchmark>` (`$PGPI_BENCH_OUT/<benchmark>`) | Output directory; **emptied at the start of a run** |
| `--plugins` | all bundled plugins that can run here | Comma-separated plugins (a spec such as `grasp:epochs=50` is used as given) |
| `--reps` | 3 | Runs of each query per estimator; the median is used |
| `--quick` | off | 1 run per query, shorter training, fewer queries (`--reps` is ignored) |
| `--train` | per benchmark | job_light, ceb_1a: the number of training queries (the first N) |
| `--timeout` | 30 s (stats_ceb), 60 s | `statement_timeout`; a canceled run counts as this |
| `--temp-limit` | 2GB | `temp_file_limit`; a run over it fails and counts as a timeout (`off`: the server's setting) |
| `--parallel N` | 0 | `max_parallel_workers_per_gather` for every run, training and test alike (0: no parallel query) |
| `--guard K` | off | As `serve --guard`: abstain on answers more than K times off PostgreSQL's estimate |
| `--guard-under U` | off | As `serve --guard-under`: abstain on answers more than U times below PostgreSQL's estimate |
| `--report-only` | off | Only collect and report the runs already in `pgpi.plans` (after a failed report) |
| `--load` | off | stats_ceb: recreate schema `stats` |

**Note:** Every run first empties `pgpi.plans` and `pgpi.stats`. Collect anything worth keeping beforehand.

### The scripts in `scripts/`

The scripts run benchmarks with the same settings. Each writes into `benchmark/data/<name>/` and logs to `benchmark/data/<name>.log`. A run whose output directory exists is refused, so nothing is overwritten.

| Script | Run |
| --- | --- |
| `stats_ceb.sh` | stats_ceb |
| `job_light.sh` | job_light |
| `ceb_1a.sh` | ceb_1a |
| `all.sh` | the three above, one after another |

```sh
export DSN="..."
nohup bash benchmark/scripts/all.sh > /dev/null 2>&1 &
```

The settings are in `scripts/common.sh` (`PLUGINS`, `--temp-limit 3GB`, `DATA`). Each can be overridden from the environment:

```sh
PLUGINS=feedback,hres DATA=/tmp/b bash benchmark/scripts/all.sh
```

## What a run does

A run has five steps.

1. The data is loaded or checked (see each benchmark below).
2. `train.sql` runs with PostgreSQL alone. The plans are captured (label `<prefix>-train`) and collected into the dataset `ds/`.
3. Every estimator is fitted on that dataset and gets an estimation server of its own.
4. `test.sql` runs with each estimator (`pgpi.mode = enforce`, label `<prefix>-<plugin>`) and with PostgreSQL alone (`<prefix>-postgres`). The queries go one by one, the estimators in a shuffled order, `--reps` times.
5. The test runs are collected into `ds/` and reported.

The prefixes are `stats`, `jl` and `ceb`. Parallel query is off (`max_parallel_workers_per_gather = 0`; `--parallel N` sets N) and `random_page_cost = 1.1`. These hold for every run, training included.

## Output

`--out` has these files:

- `report.txt`: the report. For each estimator, for A, B and all, it has
  - the sum over the queries of the median execution time (server side, `pgpi.plans.execution_ms`) and its ratio to PostgreSQL's,
  - the number of queries whose plan differs from PostgreSQL's,
  - the timeouts,
  - the median and p90 q-error of the estimates the planner used.

  A run counts as `--timeout` if it was canceled at the timeout, went over `--temp-limit`, or was canceled while still being planned (no plan captured).
- `per_query.tsv`: every test query, every estimator.
- `failures.tsv`: the runs that failed (over `--temp-limit`).
- `output.txt`: what was printed.
- `ds/`: the Parquet dataset of the training and the test runs.

## stats_ceb

A subset of STATS-CEB (Han et al., "Cardinality Estimation in DBMS: A Comprehensive Benchmark Evaluation", PVLDB 15, 2022) on the STATS data: simplified Stack Exchange data, 8 tables, about 1 million rows.

The queries are the 146 `COUNT(*)` queries of STATS-CEB, restricted to those that take at most 5 s with PostgreSQL alone, and at most 2 per join graph. They are split by join graph:

- **A** (57): join graphs seen in training. `test.sql` has the original STATS-CEB queries. `train.sql` has 10 variants of each, with other constants (same tables, joins and filtered columns).
- **B** (32): join graphs held out. No training query has their tables and joins.

The first run downloads the data (about 40 MB, from the STATS-CEB repository at a fixed commit, into `~/.cache/pgpi-demo/stats`) and loads it into schema `stats`. The timeout is 30 s.

## job_light

JOB-light (Kipf et al., "Learned Cardinalities: Estimating Correlated Joins with Deep Learning", CIDR 2019) has 70 `COUNT(*)` queries over 6 tables of the IMDb data of JOB (Leis et al., PVLDB 9, 2015). Each joins `title` with 1 to 4 of `cast_info`, `movie_companies`, `movie_info`, `movie_info_idx` and `movie_keyword`, with predicates on integer columns.

The training queries come from the MSCN training set (`train.sql`: the first 630, or 210 with `--quick`). They have 0 to 2 joins. The test queries are split by size:

- **A** (35): at most 3 tables. Their join graphs are those of the training queries.
- **B** (35): 4 or 5 tables. More joins than any training query.

The timeout is 60 s. The workload is a star of joins on `title`, so the plans change little with any estimator. Most differences between estimators are within the run-to-run variation.

## ceb_1a

Template 1a of CEB-IMDb (Negi et al., "Flow-Loss: Learning Cardinality Estimates That Matter", PVLDB 14, 2021). Every query joins 9 tables (`title`, `kind_type`, two `movie_info` with their `info_type`, `cast_info`, `role_type`, `name`). The predicates are correlated across tables. They filter on the kind of title, two kinds of movie information (genres and languages, for example), the role, the gender and a range of production years.

`queries.txt` names the queries used, chosen from the 3000 of the template. All have different literals.

- **train** (500, or 200 with `--quick`): queries of the five pairs of information kinds that make up most of 1a.
- **A** (100, or 50): other queries of the same five pairs.
- **B** (60, or 30): queries with other pairs of information kinds. No training query has them.

The first run downloads the queries into `~/.cache/pgpi-demo/ceb`. They come from a copy of CEB's SQL in [imdb_pg_dataset](https://github.com/RyanMarcus/imdb_pg_dataset) (`ceb-imdb-13k`, fixed commit; CEB's own links no longer work). They are not bundled. If you use them, cite the Flow-Loss paper (see `lib/ceb.py`). The timeout is 60 s.
