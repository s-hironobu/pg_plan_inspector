#!/usr/bin/env python3
"""A subset of STATS-CEB: estimators compared by execution time on a real data set.

STATS-CEB (Han et al., "Cardinality Estimation in DBMS: A Comprehensive Benchmark
Evaluation", PVLDB 15, 2022) runs 146 COUNT(*) queries over the STATS data set
(Stack Exchange, 8 tables, about 1 million rows). This benchmark uses the queries
that take at most 5 s with PostgreSQL alone. They are split by join graph:

  A  join graphs seen in training: test.sql has the original STATS-CEB queries,
     train.sql 10 variants of each with other constants (same tables, joins and
     filtered columns)
  B  join graphs held out: no training query has their tables and joins

The data is downloaded once (lib/stats.py) and loaded into schema stats. The steps
and the report are those of lib/bench.py.

    python3 benchmark/stats_ceb/run.py --dsn ...
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import lib  # noqa: E402,F401  (makes client/ importable without PYTHONPATH)
from lib.bench import Bench, main  # noqa: E402
from lib.runner import prepare  # noqa: E402


def load(conn, a, say):
    prepare(conn, "stats", reload=a.load, say=say)


BENCH = Bench(name="stats_ceb", here=HERE, prefix="stats", data_title="STATS data (schema stats)",
              prepare=load, search_path="stats, public", timeout=30,
              extra_args=[(["--load"], {"action": "store_true",
                                        "help": "(re)create schema stats first (loaded anyway if missing)"})])

if __name__ == "__main__":
    main(BENCH, __doc__)
