# Samples

The data and the outputs behind [ANALYSIS.md](../ANALYSIS.md) and [RESULTS.md](../RESULTS.md). With them, you can run every analysis command without PostgreSQL, the IMDb data or a benchmark run.

```
samples/
  data/ceb_1a/      report.txt, ds/
  data/job_light/   report.txt, ds/
  data/stats_ceb/   report.txt, ds/
  output/           the outputs of the analysis commands (16 files)
```

- `data/<benchmark>/ds/` is the dataset that the benchmark collected (Parquet; see [DATASET.md](../../client/pgpi/DATASET.md)). It has the plans of every query under every runner, the nodes with their actual rows, and the requests of the estimators. It has no table data.
- `data/<benchmark>/report.txt` is the report of the same run.
- `output/` has the files that the commands of ANALYSIS.md write. The line numbers in ANALYSIS.md refer to them.

## Use

Run in `client/`. Point `--data` at a `ds/` here:

```sh
python3 tools/slow_queries.py --data ../benchmark/samples/data/ceb_1a/ds \
    --estimator-label ceb-litecard --top 3 > slow_litecard.txt
diff slow_litecard.txt ../benchmark/samples/output/slow_litecard.txt
```

The slow-query lists and the q-error tables are the same on every run. The fits of `nngp` and `grasp` are not: a new fit gives numbers close to those in `output/`, not equal. Compare the pattern, not the digits.

If you ran the benchmarks yourself, your `ds/` is in `benchmark/data/<benchmark>/ds`. Use that path instead.

## The run

The run of [RESULTS.md](../RESULTS.md): the plugins `feedback`, `hres`, `matcomp`, `ce-feedback`, `litecard`, `nngp` and `grasp` (the benchmarks use `grasp:epochs=100,restarts=1`), and the baseline `postgres`. The labels are `<prefix>-train`, `<prefix>-postgres` and `<prefix>-<plugin>` with the prefixes `stats`, `jl` and `ceb`.

## Sources of the queries

The queries come from the public benchmarks STATS-CEB, JOB-light and CEB. See the [benchmark README](../README.md) for their sources and licenses.
