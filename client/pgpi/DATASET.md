# Dataset format

A dataset made by `python3 -m pgpi collect` is a directory of Parquet files with four core tables: `dataset.plans`, `dataset.nodes`, `dataset.requests` and `dataset.stats` in this document. They are files of the client, not tables in PostgreSQL. The extension's log tables in PostgreSQL are `pgpi.plans` and `pgpi.stats` ([7.1.4](../../README.md#714-log-tables)):

```
ds/
  manifest.json        last collected seqid, source, anonymization
  plans/part-*.parquet       one row per executed statement
  nodes/part-*.parquet       one row per plan node, with ready-to-use columns
  requests/part-*.parquet    intervention requests stored with the plans
  stats/part-*.parquet       statistics contents, one row per (dbid, relid, version)
```

Load the tables as pyarrow.Table objects (convertible to pandas with `.to_pandas()`) or query them with DuckDB:

```python
ds = pgpi.Dataset("ds/")
ds.plans(); ds.nodes(); ds.requests(); ds.stats()     # optional: columns=[...], filter=...
con = ds.duckdb()                                      # views plans, nodes, requests, stats
con.sql("SELECT point, count(*) FROM nodes WHERE usable GROUP BY point")
```

Most estimators work mainly with `examples()`. It yields one `pgpi.Example` per usable plan node.

## Examples

`ds.examples(points=None, labels=None)` extracts training examples from `dataset.nodes`:

| Attribute | Description |
|---|---|
| `request` | The cardinality question as a `pgpi.Request` - identical to the object passed to `estimate()` during live planning |
| `true_rows` | Observed row count, normalized to the request's basis (rows per loop for `param_base` / `param_join`) |
| `est_pg` | PostgreSQL's native estimate |
| `est_plan` | The value actually utilized by the planner (differs from `est_pg` if an intervention was applied) |
| `seqid`, `node_id` | Plan and node identifiers |
| `label`, `queryid`, `planid` | Descriptive labels and tracking IDs |
| `censored`, `nloops` | Flags whether execution terminated early (treating `true_rows` as a lower bound; excluded by default), along with loop counts |

## Request

A `pgpi.Request` mirrors the JSON payload sent to the estimation server ([7.2](../../README.md#72-pgpi-server)):

| Attribute | Description |
|---|---|
| `point` | Intervention point: `base`, `join`, `param_base`, `param_join`, or `clauselist` |
| `rels` |  Target relations as a tuple of `Rel(alias, rel, relid, rti, inh)`. `rel` is the schema-qualified table name (None for non-tables like subqueries or functions) |
| `outer` |  Outer relations for parameterized questions (the `y` in "rows of `x` per row of `y`") |
| `clauses` | Restriction and join clauses on target relations: tuple of `Clause(sql, rels)` (`sql` is executable SQL or `None` if deparsing failed) |
| `outer_clauses` | Clauses acting on outer relations |
| `jointype` | PostgreSQL `JoinType` integer (0 = inner) |
| `pg` | PostgreSQL's native estimate (rows, or selectivity for `clauselist`) |
| `tuples` | Total tuples in the relation (base and clauselist) |
| `aliases`, `tables`, `n_rels`, `is_param` | Utility properties |
| `key()` | Unique question identifier including literals (for exact matching) |
| `template()` | Normalized question identifier with literals replaced by `?` (query class) |
| `count_sql()`, `param_sqls()` | SQL queries used by `oracle` to compute true counts |
| `data` |  Raw JSON dictionary. Live requests include additional fields: `data.get("hints")` (`pgpi.hints` value), and for join requests `data.get("split")`, `data.get("outer_rows")`, `data.get("inner_rows")` (planner split parameters and input sizes) |

## Table `dataset.plans`

Contains the columns of the PostgreSQL table `pgpi.plans` ([7.1.4](../../README.md#714-log-tables)), with metadata extracted into top-level columns:

| Column | Description |
|---|---|
| `mode` | Value of `pgpi.mode` active during planning |
| `n_nodes` | Total count of plan nodes |
| `stats_refs` | List of `{relid, rel, version}` capturing statistics versions used during planning |

The raw `meta` and `explain` payloads are kept as JSON text.

## Table `dataset.nodes`

One row per plan node, joined with `dataset.plans` via `seqid`:

| Column | Description |
|---|---|
| `seqid`, `node_id`, `parent`, `role` | Plan identifiers and node hierarchy roles (`outer`, `inner`, `member`, `subquery`, `initplan`, `subplan`) |
| `type` | Node type string (`Seq Scan`, `Hash Join`, ...) |
| `label`, `queryid`, `planid` | Copied from the parent plan |
| `point` | Corresponding intervention point (`NULL` for non-cardinality nodes like Sort, Hash, or Aggregate) |
| `relids`, `ident`, `tables`, `n_rels` | Underlying relations (`alias#n/schema.partition@subquery`, names, count) |
| `param_outer` | RTIs of outer relations for parameterized nodes |
| `plan_rows`, `startup_cost`, `total_cost`, `width` | Standard EXPLAIN metrics |
| `parallel_aware`, `partial`, `parallel_divisor` | Parallel execution flags (`partial` indicates worker-split rows) |
| `ntuples`, `nloops`, `done` | Raw runtime execution counters |
| `worker_ntuples`, `worker_loops`, `nfiltered1`, `nfiltered2` | Worker execution and filter-removal counters |
| `censored`, `censor_basis` | Indicates early termination (e.g., `LIMIT`, `semi-joins`), marking true_rows as a lower bound |
| `true_rows` | Observed execution rows (normalized) |
| `est_pg`, `est_plan` | Native PostgreSQL estimate vs. plan-used estimate |
| `est_answer`, `est_source`, `est_applied`, `est_route` |  Intervention telemetry: returned answer, source (`directive`, `callback`, etc.), application status, and route |
| `q_error_pg` | Q-error of PostgreSQL's native estimate |
| `usable` | Boolean indicating whether a cardinality node was executed, uncensored, and eligible for training |
| `ce`, `ce_key`, `ce_template` | Descriptor JSON and identifier strings (`key()` / `template()`) |
| `relinfo`, `index` | JSON snapshots of relation pages, tuples, visibility fractions, and index dimensions |

The values of `true_rows`, `est_pg` and `est_plan` are normalized to one basis, so they are directly comparable:
- `param_base` / `param_join`: Rows per loop (`ntuples / nloops`).
- Partial nodes (beneath a Gather): Total aggregated rows across all workers (`ntuples`) matched against the planner's whole-relation estimate.
- All other nodes: Rows per loop.

## Table `dataset.requests`

The intervention requests saved with each plan. `collect` extracts them from the `meta` column of `pgpi.plans`. The GUC `pgpi.log_requests` sets which requests are saved. The columns are `seqid`, `seq` (order within the planning), `node_id` (the node it matched, if any), `point`, `route`, `pg`, `answer`, `source`, `applied`, `key`, `template`, and `request` (the full request JSON).

## Table `dataset.stats`

One row per unique (`dbid`, `relid`, `version`) tuple, with `database`, `rel`, `captured_at` and a `stats` JSON document. `ds.stats_of(dbid, relid, version)` parses it. Structure summary:

```json
{"relid": 24601, "name": "shop.customers", "relkind": "r",
 "reltuples": 50000, "relpages": 319, "relallvisible": 319, ...,
 "columns": [
   {"attnum": 2, "attname": "region", "type": "text", "inh": false,
    "null_frac": 0, "avg_width": 3, "n_distinct": 20,
    "slots": [{"kind": 1, "kind_name": "mcv", "op": 98, "coll": 100,
               "numbers": [0.0512, 0.0507, ...], "values_type": "text",
               "values": ["r3", "r17", ...]}]},
   ...],
 "extended": [{"oid": ..., "name": ..., "inh": false, "ndistinct": ..., "dependencies": ...,
               "has_mcv": true, "has_expr_stats": false}],
 "indexes": [...]}
```

`columns` mirrors `pg_statistic`, with one entry per column and inheritance flag `inh`. Slots specify sampling metrics (`mcv`, `histogram`, `correlation`), operators, collations, and frequency and value lists. To see the statistics of a specific plan, follow `dataset.plans.stats_refs` to the corresponding version entry.
