# Analysis

`ds/` is not only for the report. It also supports further analysis.

A report says how much slower or faster a method was. It does not say why. `ds/` can. It has every plan of every query under every runner, with the estimate and the actual rows of every node. The scripts in `client/tools/` read it without a database.

This page shows the procedure of analysis with three cases, on the ceb_1a and stats_ceb runs of the [results](RESULTS.md).

For convenience, the data is already prepared. Run all commands in `client/`. They read [samples/](samples/README.md), so no benchmark run is needed.

If you ran the benchmarks yourself, use your own `benchmark/data/<benchmark>/ds`.

## Procedure

The typical procedure has four steps, from the totals down to one plugin:

1. **The totals** (`report.txt`). Which methods are slower or faster than PostgreSQL, on A (covered by the training) and on B? How many plans changed? How many timeouts?

2. **The q-error distributions** (`tools/qerror_runs.py`). Are the errors large overall, in the tails (p99), or at some join sizes? Each label is scored on its own plans.

3. **The slow queries** (`tools/slow_queries.py`).
   - It lists the queries that the method made slowest, against PostgreSQL in the same run. Each has both plans as trees. Every node shows:
     - the estimate,
     - the actual rows,
     - the q-error (`v` under, `^` over).
   - Read the plan of the method from the bottom up. Find the lowest node with a large error. The errors above it usually follow from it.
   - Compare with the plan of PostgreSQL.
     - A large underestimate low in the plan leads to nested loops over far larger inputs (timeouts).
     - A large overestimate leads to hash joins over small inputs (slower, rarely catastrophic).
   - `(stopped early)` marks a canceled run. The actual rows, and so the q-error, are lower bounds.

4. **The plugin's own diagnosis.** Once the node is known, ask the plugin about it offline:
   - `python3 -m pgpi qerror --plugin P --fit-label L --label L`: the q-error of the plugin on the nodes of another label. For example, the plans of the baseline (`--label <prefix>-postgres`).

   - `tools/qerror_by_source.py`: the q-error by the source of the answer. For litecard it also shows why it abstained.

   - `tools/grasp_check.py`: a check that splits an estimate of grasp into its parts.

   - A short script on `pgpi.Dataset`, for what the tools do not cover. Example: count the nodes of a set of tables per label, to see whether the training data has the subquery.

   These checks read `ds/` only. The benchmark does not need to run again.

   Fit on the training label and ask about the nodes of the slow plans. One of three cases shows up:
   - Wrong on the training nodes: a problem of fitting.

   - Right on them, wrong on new literals or combinations: a problem of generalization.

   - The training never has the subquery, because PostgreSQL never executes it.


Every output in a case comes from the `ds/` of the run that the case names.

## Case: litecard on ceb_1a

All commands run in `client/`. Each saves its full output in a file. The line numbers refer to these files.

### Step 3: the slow queries

```sh
python3 tools/slow_queries.py --data ../benchmark/samples/data/ceb_1a/ds \
    --estimator-label ceb-litecard --top 3 > slow_litecard.txt
```

The output `slow_litecard.txt` has 160 lines. Line 1 is the summary:
- 98 of the 160 queries are at least 100 ms slower than with PostgreSQL, 117,659 ms in total.
- The total of all queries is 480,491 ms against 397,319 ms (1.21).

The three slowest queries start at lines 3, 55 and 108. They take 2.5 to 4.1 times as long as with PostgreSQL (+4.8 to +5.3 s), with different plans. Each block has the SQL, the plan with litecard, the plan with PostgreSQL and the worst estimates of litecard.

The plans fail by **overestimates**. The largest errors are at the top joins of 7 relations (q 206, 15385 and 14789), but each starts lower in the plan:

#### Query 1 (lines 3 to 53, plan with litecard: lines 5 to 27)
The join of 5 relations (line 13) is estimated at 7.89e7 rows against 994,983 (q 79), while its input of 4 relations (line 14) is off by only 3.

The error enters at 5 relations (4 joins) and the joins above carry it: q 18 at 6 relations (line 12), q 206 at 7 relations (line 11). Here the bias correction multiplies by 68656 (Table 5 of the paper). Step 4 shows that the correction answers 90% of the joins of 5 relations.

#### Query 2 (lines 55 to 106, plan with litecard: lines 57 to 80)
The first large error is at 4 relations (line 65): est 3.29e7 against 142,596 (q 230), while its input of 3 relations is nearly exact (line 66).

Table 5 of the paper has the factor 1361 for 3 joins. Above it: q 1762 at 6 relations (line 64), q 15385 at 7 relations (line 63).

#### Query 3 (lines 108 to 159, plan with litecard: lines 110 to 133)
The join of 4 relations is fine (q 2, line 118).

The error enters at 6 relations (line 117): est 5.72e7 against 41,322 (q 1385). The top join has q 14789 (line 116).

Table 5 of the paper has no entry for 5 joins, so this estimate is not a bias-corrected one. `slow_queries.py` does not show where it came from.

Lines 116 to 118 of `slow_litecard.txt` (the prefix `imdb.` of the table names is removed):

```
              Hash Join [cast_info kind_type movie_info movie_info name role_type title]  est 1.30e+07  actual 879     q 14788^
                Hash Join [cast_info kind_type movie_info movie_info role_type title]     est 5.72e+07  actual 41322   q 1385^
                  Hash Join [cast_info movie_info movie_info role_type]                   est 885268    actual 368735  q 2^
```

The overestimates lead to hash joins that read whole tables. In query 1, the plan with litecard scans `title` and `name` completely (lines 19 and 25).

The plan with PostgreSQL uses index scans (lines 42 and 47). Its estimates are too low (q 331 at the top, line 30), but its plan is faster.

### Step 4: the source of every answer

```sh
python3 tools/qerror_by_source.py --data ../benchmark/samples/data/ceb_1a/ds --plugin litecard \
    --fit-label ceb-train --label ceb-litecard --by-size > source_litecard_on.txt
```

litecard is fitted on `ceb-train`. Every node of `ceb-litecard` is asked alone. For each size (`point`) and source of the answer, the table shows the q-error of litecard. After `|` it shows the q-error of PostgreSQL on the same nodes.

`source_litecard_on.txt` has 63 lines. Lines 3 to 30 are the table. Lines 32 to 63 say why litecard abstained.

Lines 3 and 7 to 23 (joins of 2 to 7 relations) are shown below:

```
point       source                n  share      gmean   median       p90   | postgres    gmean   median       p90
join2       abstain             121    22%       8.52     5.70     126.9   |              8.52     5.70     126.9
join2       litecard-bias       156    29%       7.69     5.70      68.2   |              6.94     4.89     107.0
join2       litecard-h2          12     2%       2.76     4.42       4.7   |              4.58     4.09       6.8
join2       litecard-h3         250    46%       2.11     1.66       5.4   |              3.20     2.69      12.6
join3       abstain              61    12%      17.11     9.33     336.2   |             17.11     9.33     336.2
join3       litecard-bias       341    68%       9.40     7.16     102.4   |             13.71    10.00     249.1
join3       litecard-h3          98    20%       4.62     3.42      16.4   |              2.50     2.01       4.9
join4       abstain              33     7%      23.59    20.86     246.4   |             23.59    20.86     246.4
join4       litecard-bias       401    83%      88.35    68.36    3601.1   |             26.17    21.34     352.0
join4       litecard-h3          47    10%       4.63     4.38      55.0   |              3.43     3.15      18.3
join5       abstain               5     1%      15.98    14.09      73.3   |             15.98    14.09      73.3
join5       litecard-bias       384    90%   42175.43 39625.05 1134477.9   |              8.97     6.24      80.9
join5       litecard-h3          38     9%       5.02     3.18      51.5   |             18.81    12.52     149.7
join6       abstain             441    97%      73.72    51.19    6053.1   |             73.72    51.19    6053.1
join6       litecard-h3          15     3%       7.98     9.69      26.2   |              7.31     7.39      24.1
join7       abstain             462    97%     128.02    88.19   13428.0   |            128.02    88.19   13428.0
join7       litecard-h3          15     3%       6.26     6.71      42.9   |              2.76     3.77       4.8
```

Conclusions:

1. **The bias correction (`litecard-bias`) answers most joins of 2 to 5 relations.** The shares are 29%, 68%, 83% and 90% (lines 8, 12, 15 and 18).
2. **From 4 relations on, its answers are worse than PostgreSQL's.** On the same nodes, the median q-error is 68 against 21 at 4 relations (line 15) and 39625 against 6.2 at 5 relations (line 18). At 3 relations it is slightly better (7.2 against 10.0), at 2 relations similar (5.7 against 4.9).
3. **This links to step 3.** The errors of the slow queries enter at 5 relations (query 1) and 4 relations (query 2), the sizes that the correction answers. The overestimates lead to hash joins that read whole tables. The tools do not show directly that each node is a corrected answer.
4. **The learned answers (`litecard-h2`, `litecard-h3`) are fine.** The median is 1.7 to 9.7 (lines 10, 13, 16, 19, 21 and 23), similar to PostgreSQL's or better (better at 2 and 5 relations).
5. **From 6 relations on, litecard answers only 2 to 3% of the joins** and leaves the rest to PostgreSQL (`abstain`, lines 20 and 22). Lines 49 to 51 say why at 6 relations: of the 441 abstained joins, 375 have a pattern that was never seen at any level. The others were seen fewer times than the thresholds. So litecard has no effect there, and no part in the slowdown.

In one sentence: the main cause of the slowdown is not what litecard has learned but the bias correction with the factors of Table 5 of the paper, which answers worse than PostgreSQL from 4 relations on.

This is a result per node. It does not show the plans and times that would follow without the correction.


## Case: nngp on stats_ceb

All commands run in `client/`. Each saves its full output in a file. The line numbers refer to these files. `stats-train`, `stats-nngp` and `stats-postgres` are the labels of the stats_ceb run.

### Step 3: the slow queries

```sh
python3 tools/slow_queries.py --data ../benchmark/samples/data/stats_ceb/ds \
    --estimator-label stats-nngp --top 3 > slow_nngp.txt
```

The output `slow_nngp.txt` has 97 lines. Line 1 is the summary:
- 10 of the 89 queries are at least 100 ms slower than with PostgreSQL, 9,552 ms in total.
- The total of all queries is 45,951 ms against 44,812 ms (1.03). The slowdowns are small next to those of grasp on ceb_1a.

The three slowest queries start at lines 3, 38 and 69. All have a different plan from PostgreSQL's.

| Query | Lines | nngp | PostgreSQL | Ratio |
|---|---|---|---|---|
| 1 | 3 to 36 | 6,276 ms | 1,103 ms | 5.7 |
| 2 | 38 to 67 | 1,710 ms | 208 ms | 8.2 |
| 3 | 69 to 97 | 4,560 ms | 3,074 ms | 1.5 |

Each block has the plan with nngp (lines 5 to 16, 40 to 49, 71 to 80), the plan with PostgreSQL (lines 17 to 30, 50 to 61, 81 to 91) and the worst estimates of nngp.

#### Query 1

The errors go both ways:

- The top join of 5 relations is overestimated: est 4.62e8 against 2.86e6 (q 161, line 7).
- Its input of 4 relations is underestimated (q 37, line 8), and so is an index scan of `posts` (q 166, line 16; stopped early, so a lower bound).

The plan of PostgreSQL also overestimates at the top (q 16, line 19).

#### Query 2

The error starts at the join of 2 relations `badges ⋈ users` (line 44): est 232 against 65,475 (q 282).

Lines 40 to 44 of `slow_nngp.txt` (query 2; the prefix `stats.` of the table names is removed):

```
Nested Loop [badges posthistory posts users]  est 360    actual 1.94e+06  q 5380v
  Nested Loop [badges posts users]            est 37     actual 203957    q 5512v
    Hash Join [badges users]                  est 232    actual 65475     q 282v
```

It grows when `posts` is joined (line 43, q 5512) and at the top (line 42, q 5380). In the plan of PostgreSQL, no node has q above 5 (lines 52 to 56).

#### Query 3

The top join of 4 relations is underestimated: est 580,192 against 5.64e7 (q 97, line 73).

The `votes` scan below it has q 109 (line 80, stopped early, a lower bound), and its input of 3 relations has q 24 (line 74). PostgreSQL estimates the top join at 3.16e7 (line 83) and uses a hash join.

### Step 4: the diagnosis of nngp

**4.1: the q-error on the plans of PostgreSQL**

```sh
python3 -m pgpi qerror --data ../benchmark/samples/data/stats_ceb/ds \
    --plugin nngp --fit-label stats-train \
    --label stats-postgres --group-by rels > nngp_qerror_postgres.txt
```

nngp is fitted on `stats-train` and asked about the nodes of the plans of PostgreSQL:

- Line 2: it answers 80% of them and abstains on the rest.
- Line 3: it is worse than PostgreSQL (by more than 1.1) on 58% of the nodes.

Lines 7 to 18:

| Relations | median, PostgreSQL | median, nngp | p99, PostgreSQL | p99, nngp |
|---|---|---|---|---|
| 1 | 1.00 | 1.52 | 12.93 | 24.19 |
| 2 | 1.28 | 2.39 | 20.42 | 484.31 |
| 3 | 5.18 | 3.62 | 838.47 | 1526.91 |
| 4 | 4.55 | 4.33 | 4155.03 | 280362.41 |
| 5 or more | 15.29 | 5.51 | 662425.45 | 62548.80 |
| all | 1.25 | 1.97 | 2065.07 | 1777.69 |

nngp is less accurate at 1 and 2 relations.

From 3 relations on the medians are about equal or lower, but the tails are long: the p99 at 4 relations is 280,362 (line 14).

**4.2: the error by the source of the answer**

```sh
python3 tools/qerror_by_source.py --data ../benchmark/samples/data/stats_ceb/ds \
    --plugin nngp --fit-label stats-train --label stats-nngp \
    --by-size > nngp_source.txt
```

Each node of the plans of nngp is asked alone. When nngp abstains, the planner uses its own estimate.

Lines 4 to 15, for the nodes that nngp answered (against PostgreSQL on the same nodes):

| Joins of | n | share | median, nngp | median, PostgreSQL | p90, nngp | p90, PostgreSQL |
|---|---|---|---|---|---|---|
| 2 relations | 279 | 97% | 2.92 | 1.95 | 40.4 | 13.4 |
| 3 relations | 210 | 93% | 4.42 | 4.84 | 81.2 | 36.6 |
| 4 relations | 126 | 81% | 3.28 | 12.92 | 299.3 | 1159.9 |
| 5 relations | 60 | 91% | 11.06 | 16.17 | 332.6 | 7729.3 |
| 6 relations | 18 | 60% | 4.72 | 8.64 | 30.0 | 42.5 |

- At 2 and 3 relations, nngp is not better than PostgreSQL, and its p90 is worse. From 4 relations on, its median is lower, and at 4 and 5 relations its p90 is far lower.

- From 2 to 6 relations (lines 5, 7, 9, 11, 13), the share of abstentions is 3%, 7%, 19%, 9% and 40%. These nodes are hard for PostgreSQL too (median 14.80 at 4 relations, line 9; 356.34 at 5, line 11). On `param_base` nngp always abstains (line 15).

- On single-table nodes (`base`, line 4), the median is 1.34 against 1.00 for PostgreSQL.

**4.3: the q-error on the training nodes**

```sh
python3 -m pgpi qerror --data ../benchmark/samples/data/stats_ceb/ds \
    --plugin nngp --fit-label stats-train \
    --label stats-train --group-by rels > nngp_qerror_train.txt
```

The command is the same as in 4.1, but nngp is asked about the nodes it was fitted on. Line 2: it answers 80% of them. Lines 7 to 18:

| Relations | median, nngp | p99, nngp | max, nngp | p99, nngp in 4.1 (plans of PostgreSQL) |
|---|---|---|---|---|
| 1 | 1.57 | 235.13 | 7559.9 | 24.19 |
| 2 | 1.73 | 199.55 | 1237.6 | 484.31 |
| 3 | 1.64 | 355.26 | 2501.8 | 1526.91 |
| 4 | 1.81 | 386.75 | 7922.5 | 280362.41 |
| 5 or more | 1.52 | 223.94 | 264.1 | 62548.80 |
| all | 1.62 | 256.77 | 7922.5 | 1777.69 |

- On the training nodes, the median is 1.5 to 1.8 at every size, and the p99 stays below 390 from 2 relations on. On the nodes of the plans of PostgreSQL (4.1), the p99 is about 730 and 280 times larger at 4 and at 5 or more relations, and 4 times larger at 3.
- From 2 relations on, nngp is much more accurate than PostgreSQL on its training nodes (lines 9 to 16). The exception is a single table: median 1.57 against 1.02 (lines 8 and 7).

So nngp is right on the nodes it saw and wrong on some it did not see. This is the second case of the procedure: a problem of generalization, not of fitting.

The tools do not show which part of a wrong node is new: its relations, join pairs or literals. nngp abstains on a new set of relations (`SEEN_RELATIONS`).

## Case: grasp on ceb_1a

This case did not reach a conclusion. It narrows the problem down to one subquery and one factor of the estimate, but not to its cause.

It is shown because it is a good example of what the framework can do. Read it as a demonstration of the analysis tools, not as an evaluation of grasp.

All commands run in `client/`. Each saves its full output in a file. The line numbers refer to these files. The fits use `grasp:epochs=100,restarts=1`, as the benchmark does, except in the supplement.

### Step 3: the slow queries

```sh
python3 tools/slow_queries.py --data ../benchmark/samples/data/ceb_1a/ds \
    --estimator-label ceb-grasp --top 3 > slow_grasp.txt
```

The output `slow_grasp.txt` has 156 lines.

Line 1 is the summary:
- 111 of the 160 queries are at least 100 ms slower than with PostgreSQL, 831,241 ms in total.
- The total of all queries is 1,164,396 ms against 397,319 ms (2.93).

The three slowest queries start at lines 3, 54 and 106. All have a different plan from PostgreSQL's.

| Query | Lines | grasp | PostgreSQL | Ratio |
|---|---|---|---|---|
| 1 | 3 to 52 | 60,000 ms (canceled) | 906 ms | 66.2 |
| 2 | 54 to 104 | 50,359 ms | 809 ms | 62.2 |
| 3 | 106 to 156 | 49,985 ms | 3,933 ms | 12.7 |

Each block has the plan with grasp (lines 5 to 25, 56 to 77, 108 to 129), the plan with PostgreSQL (lines 26 to 46, 78 to 98, 130 to 150) and the worst estimates of grasp.

All three plans of grasp start from `title ⋈ kind_type` (lines 16, 67 and 119). Lines 13 to 16 of `slow_grasp.txt` (query 1; the prefix `imdb.` of the table names is removed):

```
Nested Loop [cast_info kind_type name role_type title]  est 3491    actual 6.47e+06  q 1854v
  Hash Join [cast_info kind_type role_type title]       est 8618    actual 6.48e+06  q 752v
    Nested Loop [cast_info kind_type title]             est 562322  actual 1.97e+07  q 35v
      Hash Join [kind_type title]                       est 29536   actual 1.36e+06  q 46v
```

The error starts at the lowest join, `title ⋈ kind_type`: q 46 in query 1 (line 16), q 40 in query 2 (line 67). In query 3 it is smaller (q 5, line 119), and so is the next one (q 4, line 118).

In all three, the next join reads `cast_info` by an index scan, once per input row. It reaches 1.97e7 rows in query 1 (line 15), 2.35e7 in query 2 (line 66) and 1.14e7 in query 3 (line 118).

- In query 1, joining `role_type` cuts the estimate by a factor of 65 (line 15 to 14), but the actual rows by 3. The result is q 752 at 4 relations and q 1854 at 5 relations (line 13).
- In queries 2 and 3, a scan of `name`, a single table, is underestimated: est 3,127 against 961,555 (q 308, line 73) and est 28,713 against 1.74e6 (q 61, line 125).
- The plan of PostgreSQL starts from a filtered scan of `movie_info` (lines 36, 88 and 142: 19,623, 1,323 and 177,351 rows).
  - In queries 1 and 2, no join of this plan returns more than 20,000 rows (lines 28 to 46 and 80 to 98).
  - In query 3, the largest has 889,792 rows (line 134).
  - In all three, joins in the plans of grasp return millions of rows (lines 13 to 16, 64 to 67, 115 to 118).

PostgreSQL is not accurate either: its top joins are underestimated by q 304, 1669 and 1273 (lines 28, 80 and 132). Its plans are still better. The time depends on the order of the joins that the estimates lead to, not on the size of the errors alone.

### Step 4: the diagnosis of grasp

The slow plans point at `title ⋈ kind_type`. Can grasp estimate it?

**4.1: the q-error on the plans of PostgreSQL**

```sh
python3 -m pgpi qerror --data ../benchmark/samples/data/ceb_1a/ds \
    --plugin grasp:epochs=100,restarts=1 --fit-label ceb-train \
    --label ceb-postgres --group-by rels > grasp_qerror_postgres.txt
```

grasp is fitted on `ceb-train` and asked about the nodes of the plans of PostgreSQL. Lines 6 to 18 of `grasp_qerror_postgres.txt`:

| Relations | median, PostgreSQL | median, grasp | p99, PostgreSQL | p99, grasp |
|---|---|---|---|---|
| 2 | 17.70 | 3.22 | 643.67 | 197.61 |
| 3 | 26.02 | 2.69 | 1022.94 | 147.04 |
| 4 | 401.72 | 3.92 | 29326.00 | 5045.33 |
| 5 or more | 373.28 | 3.92 | 75299.68 | 1143.72 |

Line 3: grasp is worse than PostgreSQL (by more than 1.1) on 18% of the nodes. Overall it is more accurate (median 1.83 against 10.39, lines 18 and 17).

This test cannot show the problem. The nodes come from plans that PostgreSQL chose, and their subqueries are not those that grasp chooses.

**4.2: does the training data have the subquery?**

A short script counts the nodes of `title ⋈ kind_type` per label.

```sh
cat > count_tkt.py <<'PYEOF'
# run in client/: python3 count_tkt.py
import sys, collections; sys.path.insert(0, '.')
import pgpi
ds = pgpi.Dataset('../benchmark/samples/data/ceb_1a/ds')
TKT = ['imdb.kind_type', 'imdb.title']
n = collections.Counter()
for ex in ds.examples(['join']):
    if sorted(ex.request.tables) == TKT:
        n[ex.label] += 1
for lab, c in sorted(n.items()):
    print(lab, c)
PYEOF
python3 count_tkt.py > count_tkt.txt
```

The output `count_tkt.txt` has 8 lines, one per label:

| Label | Nodes | Line |
|---|---|---|
| `ceb-train` | 127 | 8 |
| `ceb-postgres` | 63 | 7 |
| `ceb-grasp` | 279 | 2 |
| `ceb-feedback`, `ceb-hres`, `ceb-litecard`, `ceb-matcomp`, `ceb-nngp` | 121 to 189 | 1, 3, 4, 5, 6 |

So the training data has the subquery: 127 nodes in `ceb-train` (43 distinct questions, line 1 of `grasp_check_train.txt` in 4.3).

**4.3: the factors on the training nodes**

```sh
python3 tools/grasp_check.py --data ../benchmark/samples/data/ceb_1a/ds \
    --fit-label ceb-train --tables imdb.kind_type,imdb.title \
    --label ceb-train --save grasp_default.pkl > grasp_check_train.txt
```

The tool fits grasp and splits the estimate of each node into its factors: `c(t)` and `c(kt)` (the count of each table under its predicates) and the overlap of the sketches of the join key.

Each is shown against its actual value. `--save` keeps the model for 4.4. Lines 3 to 6 of `grasp_check_train.txt`:

```
q -40  grasp 4.29e+03  actual 1.7e+05  postgres 5.07e+04   [ceb-train]
     c(t) grasp 2.19e+05  actual 3.48e+05   ('1925'::integer < t.production_year) AND (t.production_year <= '1975'::integer)
     c(kt) grasp 0.946  actual 1   (kt.kind = 'episode'::text)
     overlap kt.id = t.kind_id: grasp 0.0207  actual 0.488
```

- grasp is wrong on its training nodes. Of the 15 worst questions (lines 3 to 62), 14 are underestimates.
- `c(kt)` is right: 0.946 against 1, 2.19 against 2, 2.85 against 3 (lines 5, 17, 25).
- `c(t)` is 1.5 to 1.7 times low (lines 4, 8, 12, 16).
- The overlap is the main error: 0.0207 against 0.488 (line 6). It depends on the kinds, hardly on the years. For `'episode'` it is 0.0207 for two year ranges (lines 6 and 10), against actual 0.488 and 0.375. For `episode` and `tv movie` it is 0.0373 for three ranges (lines 18, 22 and 30), against actual 0.323, 0.338 and 0.207.

**4.4: the nodes of the plans of grasp**

```sh
python3 tools/grasp_check.py --data ../benchmark/samples/data/ceb_1a/ds \
    --load grasp_default.pkl --tables imdb.kind_type,imdb.title \
    --label ceb-grasp > grasp_check_grasp.txt
```

The saved model is asked about the nodes of the slow plans (60 distinct questions). All 15 worst are underestimates (q -79 to -10, lines 3 to 62).

All three factors are low. Lines 3 to 6: `c(t)` 6.45e5 against 2.02e6, `c(kt)` 2.54 against 4 (a list of 4 kinds), overlap 0.0123 against 0.198.

**4.5: other settings**

Each command fits grasp again (about 7 minutes) and asks the same 43 questions of `ceb-train`.

```sh
python3 tools/grasp_check.py --data ../benchmark/samples/data/ceb_1a/ds \
    --plugin grasp:epochs=100,restarts=1,in_split=1 --fit-label ceb-train \
    --tables imdb.kind_type,imdb.title --label ceb-train > grasp_check_in_split.txt

python3 tools/grasp_check.py --data ../benchmark/samples/data/ceb_1a/ds \
    --fit-only-tables --fit-label ceb-train \
    --tables imdb.kind_type,imdb.title --label ceb-train > grasp_check_only_tables.txt

python3 tools/grasp_check.py --data ../benchmark/samples/data/ceb_1a/ds \
    --plugin grasp:labels=final,epochs=100,restarts=1 --fit-label ceb-train \
    --tables imdb.kind_type,imdb.title --label ceb-train > grasp_check_labels_final.txt

python3 tools/grasp_check.py --data ../benchmark/samples/data/ceb_1a/ds \
    --plugin grasp:n_h=128,epochs=100,restarts=1 --fit-label ceb-train \
    --tables imdb.kind_type,imdb.title --label ceb-train > grasp_check_n_h128.txt
```

| Setting | File | Worst q (line 3) | What the factors show |
|---|---|---|---|
| default (multi-hot IN lists) | `grasp_check_train.txt` | -40 | overlap about 0.02 for `'episode'`, actual 0.49 |
| `in_split=1` (each IN value on its own) | `grasp_check_in_split.txt` | -43 | `c(kt)` right (1.08 against 1, line 5), overlap still 0.037 against 0.488 (line 6) |
| `--fit-only-tables` | `grasp_check_only_tables.txt` | +2 | all factors close to actual (lines 3 to 6) |
| `labels=final` | `grasp_check_labels_final.txt` | -347,340 | estimates of 0.02 to 12 rows for actual 7.5e4 to 5.1e5; overlap 2e-5 to 4e-4 |
| `n_h=128` | `grasp_check_n_h128.txt` | +140 | overlap is 1 in all 15 questions; `c(kt)` carries the difference (0.082 against 1, line 5) |

- `in_split=1` fixes nothing here. `c(kt)` is already right with the default encoding.
- Fitted on these nodes alone, grasp reproduces them. The worst of the 43 questions is q 2 (line 3), and the overlap is close (line 6: 0.021 against 0.00855; line 10: 0.0359 against 0.0506). The model can represent the subquery. It does not learn it together with the other nodes.
- With `labels=final`, only the final cardinality of each query is a label. `title ⋈ kind_type` is an intermediate node, so it is not learned.
- A larger model does not help. The worst error grows, and the overlap saturates at 1.

These tests do not show why the joint fit fails. One possible reason: `title` has one representation for every join that contains it, and the loss averages over all nodes, so larger and more frequent joins dominate. This was not tested. The result holds for this implementation and the training data of pgpi (the nodes of executed plans). It does not measure the published method.

### Supplement: the defaults of grasp

The benchmark shortens the training to `epochs=100,restarts=1`. This script fits grasp with its defaults (300 epochs, 3 restarts) and asks about `title ⋈ kind_type` per label. Any other spec can be tried the same way, without a benchmark run.

```sh
cat > grasp_tkt.py <<'PYEOF'
# run in client/: python3 grasp_tkt.py
import sys, statistics; sys.path.insert(0, '.')
import pgpi
from pgpi.metrics import ExampleView
ds = pgpi.Dataset('../benchmark/samples/data/ceb_1a/ds')
est = pgpi.load('grasp')                                   # the spec to try
est.fit(ExampleView(ds, lambda ex: ex.label == 'ceb-train'))
TKT = ['imdb.kind_type', 'imdb.title']
def q(e, t): e, t = max(e, 1), max(t, 1); return max(e / t, t / e) * (1 if e >= t else -1)
for lab in ['ceb-train', 'ceb-postgres', 'ceb-grasp']:
    rows = []
    for ex in ds.examples(['join'], labels=[lab]):
        if sorted(ex.request.tables) != TKT:
            continue
        g = est.estimate_rows(ex.request)
        if g is not None:
            rows.append((q(g, ex.true_rows), q(ex.est_pg, ex.true_rows), g, ex.true_rows, ex.request))
    gq = [abs(r[0]) for r in rows]; pq = [abs(r[1]) for r in rows]
    print(f'{lab}: n {len(rows)}  grasp median {statistics.median(gq):.1f} max {max(gq):.0f} '
          f'(under {sum(r[0] < 0 for r in rows)})  postgres median {statistics.median(pq):.1f}')
    for r in sorted(rows, key=lambda r: -abs(r[0]))[:3]:
        cl = ' AND '.join(c.sql for c in r[4].clauses if c.sql and len(c.rels) == 1)
        print(f'   q {r[0]:+.0f}  est {r[2]:.0f}  actual {r[3]:.0f}   {cl[:200]}')
PYEOF
python3 grasp_tkt.py > grasp_tkt.txt
```

The fit takes about an hour. Lines 1, 5 and 9 of `grasp_tkt.txt` (`q` is negative for an underestimate; the three worst nodes of each label follow each line):

| Label | Nodes | grasp median | grasp max | Underestimated | PostgreSQL median |
|---|---|---|---|---|---|
| `ceb-train` | 127 | 1.7 | 31 | 108 | 3.7 |
| `ceb-postgres` | 63 | 1.6 | 7 | 54 | 3.4 |
| `ceb-grasp` | 279 | 4.3 | 26 | 273 | 3.7 |

With the defaults, the training nodes fit somewhat better (worst q -31, line 2, against -40 above). The direction stays. Of the 279 nodes of the plans of grasp, 273 are underestimated, and the median (4.3) is above that of PostgreSQL (3.7). Only a benchmark run with this setting shows whether the plans and the time change.
