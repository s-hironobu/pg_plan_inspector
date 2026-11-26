#!/usr/bin/env python3
"""The Quick Start with every estimator: section 5 of the README, automated.

The data and the workload are those of the Quick Start (demo/shop.sql, train.sql,
test.sql). The schema shop has correlated columns and skew. There are 40 training
queries of four templates (T1-T4) and 100 test queries:

  A  T1-T4 with literals not used in training
  B  T5-T10, templates not in train.sql at all

The Quick Start uses feedback as its example. This demo trains every bundled plugin
(those whose packages are installed) on the same training queries. It compares them
by execution time and q-error, with the runner of the benchmarks
(benchmark/lib/bench.py, same steps and report; see benchmark/README.md):

    python3 demo/run.py --dsn "$DSN"             # every plugin, 3 runs per query
    python3 demo/run.py --dsn "$DSN" --quick     # 1 run, shorter training
    python3 demo/run.py --dsn "$DSN" --plugins feedback,hres

The schema shop is created on the first run (it takes about a minute). The output goes to
/tmp/pgpi-demo/quickstart (--out).
"""

import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "benchmark"))
import lib  # noqa: E402,F401  (makes client/ importable without PYTHONPATH)
from lib.bench import Bench, main  # noqa: E402
from lib.runner import prepare  # noqa: E402

SEEN = {"T1", "T2", "T3", "T4"}             # the templates of train.sql


def load(conn, a, say):
    prepare(conn, "shop", reload=a.load, say=say)


def labelled(src: str, dst: str, kind=None) -> None:
    """Write src with a '-- <group> <name>' line before every query (group A/B, or train)."""
    tpl, n = None, {}
    with open(src) as f, open(dst, "w") as out:
        for line in f:
            m = re.match(r"--\s*(T\d+)\b", line)
            if m:
                tpl = m.group(1)
            elif line.startswith("SELECT"):
                n[tpl] = n.get(tpl, 0) + 1
                group = kind or ("A" if tpl in SEEN else "B")
                out.write(f"-- {group} {tpl}_{n[tpl]}\n{line}")


def workload(a, say):
    d = os.path.join(a.out, "workload")
    os.makedirs(d, exist_ok=True)
    labelled(os.path.join(HERE, "train.sql"), os.path.join(d, "train.sql"), "train")
    labelled(os.path.join(HERE, "test.sql"), os.path.join(d, "test.sql"))
    return os.path.join(d, "train.sql"), os.path.join(d, "test.sql")


BENCH = Bench(name="quickstart", here=HERE, prefix="qs", data_title="Quick Start data (schema shop, demo/shop.sql)",
              prepare=load, search_path="shop, public", timeout=30, workload=workload,
              out=os.path.join(os.environ.get("PGPI_DEMO_OUT", "/tmp/pgpi-demo"), "quickstart"),
              extra_args=[(["--load"], {"action": "store_true",
                                        "help": "(re)create schema shop first (loaded anyway if missing)"})])

if __name__ == "__main__":
    main(BENCH, __doc__)
