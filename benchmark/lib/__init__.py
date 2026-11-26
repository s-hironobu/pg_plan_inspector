"""Shared parts of the benchmarks (and of the demo, which is a small one).

Every benchmark script starts with

    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    import lib  # noqa: F401

This also puts client/ first on the path. So the benchmarks always run the pgpi of
this repository, with your changes and your plugins. No PYTHONPATH is needed, even
if another copy of pgpi is installed.
"""

import os
import sys

BENCH_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO_DIR = os.path.dirname(BENCH_DIR)
DEMO_DIR = os.path.join(REPO_DIR, "demo")

#: where a benchmark writes when --out is not given (benchmark/data/<name>)
OUT_BASE = os.environ.get("PGPI_BENCH_OUT", os.path.join(BENCH_DIR, "data"))
#: the downloads (STATS data, CEB queries), kept between runs
CACHE_BASE = (os.environ.get("PGPI_BENCH_CACHE") or os.environ.get("PGPI_DEMO_CACHE")
              or os.path.expanduser("~/.cache/pgpi-demo"))
#: the connection used when --dsn is not given: $PGPI_BENCH_DSN, then $DSN (as in the
#: Quick Start of the README), then the defaults of libpq (PGHOST, PGUSER, ...)
DEFAULT_DSN = os.environ.get("PGPI_BENCH_DSN") or os.environ.get("DSN") or ""

sys.path.insert(0, os.path.join(REPO_DIR, "client"))


def out_dir(name: str, *parts: str) -> str:
    """Return the default output path of a benchmark (OUT_BASE/<name>/...)."""
    return os.path.join(OUT_BASE, name, *parts)
