# client/tools

Four scripts that explain a finished run: which estimates were wrong, where in the plan, and why.

A benchmark report gives totals. These scripts open the dataset (`ds/`) behind it and look at single plans and nodes. For example, `slow_queries.py` finds the queries an estimator made slower, and the node where the estimate first went wrong.

Run them in `client/`, like `python3 -m pgpi`. pgpi is not installed: each script adds `client/` to the path itself. None of them changes the dataset. `grasp_check.py --save` writes the fitted model to a file.

## Overview

| Script | What it shows | Fits an estimator | Database |
| --- | --- | --- | --- |
| [`qerror_runs.py`](#qerror_runspy) | the q-error of the estimates the planner actually used, by label (and by point and join size) | no | no |
| [`slow_queries.py`](#slow_queriespy) | the queries an estimator made slower than a baseline, with both plans and the estimates in them | no | no |
| [`qerror_by_source.py`](#qerror_by_sourcepy) | an estimator's q-error on recorded nodes, by the source of its answer (pattern level, bias correction, abstention, ...) | yes | only if the plugin needs one (e.g. `oracle`) |
| [`grasp_check.py`](#grasp_checkpy) | for grasp: the estimate of a join split into its factors, against the actual values | yes (grasp) | no |

The first three are general. They work on any run of the benchmarks or the demo. The litecard breakdown in `qerror_by_source.py` applies to litecard only.

The last one is specific. It was written to investigate grasp, and it works on any run of grasp.

A typical order is: `qerror_runs.py` (how large are the errors), `slow_queries.py` (which queries, which nodes), then a diagnosis of the plugin. The procedure is in [Analysis](../../benchmark/ANALYSIS.md).

## qerror_runs.py

Summarizes how far the planner's estimates were from the actual row counts, per label.

The **q-error** is max(est/actual, actual/est). An estimate of 10 rows for an actual 1,000 gives 100. An estimate of 1,000 for an actual 10 also gives 100. A perfect estimate gives 1.

The script computes it for every usable plan node of the chosen labels, on `est_plan`: the value the planner used. That is the estimator's answer, or PostgreSQL's estimate where the estimator abstained. For a run without an estimator, it is PostgreSQL's estimate.

The benchmark reports show the median and the 90th percentile. This tool adds the geometric mean and the 99th percentile, which show the tails that those two hide.

```sh
cd client
python3 tools/qerror_runs.py --data ../benchmark/data/ceb_1a/ds
python3 tools/qerror_runs.py --data ../benchmark/data/ceb_1a/ds --like 'ceb-%' --by-point
```

| Option | Description |
| --- | --- |
| `--data DIR` | the dataset: a benchmark's `ds/`, or any dataset collected with `python3 -m pgpi collect` |
| `--like PATTERN` | only these labels, as an SQL LIKE pattern (default `%`: all) |
| `--by-point` | also one line per point: `base`, `join2`, `join3`, ... (by the number of relations), `param_base`, ... |

Example output (one line per label, and per point with `--by-point`). The numbers are measured values of one run, shown only as an example:

```
label                            point           n   gmean  median       p90       p99
ceb-hres                         all          7375    4.90    1.91      86.3    9790.8
ceb-postgres                     all          7254   14.17    4.07    1302.0   44193.4
```

- `n`: the plan nodes with an estimate and an actual row count. Canceled and failed runs leave none, or fewer.
- `gmean`: the geometric mean, that is the mean of the log q-error. `median`, `p90`, `p99`: percentiles.

The nodes are those of each label's own plans. Two estimators lead to different plans, so their lines are not about the same questions (their `n` differs too). A label of PostgreSQL alone (`*-postgres`, `*-train`) shows PostgreSQL's own errors on the same queries.

## slow_queries.py

Shows where the estimates went wrong in the plans that were slow. The totals and the q-error summaries do not show that.

The script takes two labels of the same queries: an estimator's and a baseline's. For each query, it takes the median execution time under each label. It then picks the queries with the largest excess over the baseline.

For each of them it prints:

- both plans as trees. Every cardinality node shows the estimate the planner used, the actual rows (`x N` for N loops) and the q-error. `v` marks an underestimate, `^` an overestimate, from a q-error of 2.
- the worst estimates of the estimator's plan.

```sh
python3 tools/slow_queries.py --data ../benchmark/data/ceb_1a/ds --estimator-label ceb-grasp
python3 tools/slow_queries.py --data ../benchmark/data/ceb_1a/ds --estimator-label ceb-grasp --top 3 --no-base
```

| Option | Description |
| --- | --- |
| `--estimator-label LABEL` | the estimator's runs (`ceb-grasp`, `stats-litecard`, `jl-hres`, ...) |
| `--baseline-label LABEL` | the baseline's runs (default: the prefix of `--estimator-label` with `postgres`, e.g. `ceb-postgres`) |
| `--top N` | number of queries (default 5) |
| `--min-ms MS` | smallest excess shown (default 100 ms) |
| `--nodes N` | worst estimates listed per query (default 5) |
| `--timeout-ms MS` | time counted for a run that did not complete (default 60000, the timeout of the IMDb benchmarks) |
| `--no-base` | do not print the baseline's plan |

The time is `execution_ms` of `pgpi.plans`, taken from the dataset. The plan printed for a query is the run closest to the median.

## qerror_by_source.py

Splits an estimator's q-error by where each answer came from.

The **source** is a tag the plugin attaches to an answer. For example, `litecard-h3` is an answer from the history of a level-3 pattern, and `litecard-bias` an answer with the bias correction. `abstain` marks a node where the plugin gave no answer and PostgreSQL's estimate stands.

The script fits one plugin on some labels, then asks it every node of other labels, one at a time (as `python3 -m pgpi qerror --no-propagate` does). It groups the nodes by point and by source, and prints PostgreSQL's q-error on the same nodes next to it.

For litecard, the abstained nodes are also broken down by the reason: the pattern was never seen, or it was seen fewer times than the thresholds beta.

```sh
python3 tools/qerror_by_source.py --data ../benchmark/data/ceb_1a/ds --plugin litecard \
    --fit-label ceb-train --label ceb-postgres --by-size [--dsn "$DSN"]
```

## grasp_check.py

Separates the two factors of a grasp estimate, to see which one is wrong.

grasp estimates a join as a product: the count of each table under its predicates, c(q_t), times the overlap of the learned count sketches of each join key group.

For the join nodes of the tables given with `--tables`, the script prints, per distinct question, the estimate of each factor next to the actual value:

- The actual count of a table comes from a recorded scan with the same predicates.
- The actual overlap is implied by the actual counts.

It then groups the estimates by predicates. A value the model cannot tell from others shows as a constant estimate.

Fitting takes as long as in the benchmarks. `--save` keeps the fitted model and `--load` reuses it.

```sh
python3 tools/grasp_check.py --data ../benchmark/data/ceb_1a/ds --fit-label ceb-train \
    --tables imdb.kind_type,imdb.title --save /tmp/grasp.pkl
python3 tools/grasp_check.py --data ../benchmark/data/ceb_1a/ds --load /tmp/grasp.pkl \
    --tables imdb.kind_type,imdb.title --label ceb-grasp
```

