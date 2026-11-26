"""Load the data sets, clean the observation log, and make estimator sockets."""

from __future__ import annotations

import os
import time

from . import DEMO_DIR

#: data sets: the SQL file that creates it (None: see the module), and a table that exists once it is loaded
SCHEMAS = {
    "shop": (os.path.join(DEMO_DIR, "shop.sql"), "shop.lineitems"),   # the Quick Start data (demo/)
    "stats": (None, "stats.votes"),          # downloaded: lib/stats.py
    "imdb": (None, "imdb.cast_info"),        # prepared by hand: lib/imdb.py (never downloaded by a benchmark)
}


def schema_loaded(conn, name: str) -> bool:
    if name == "imdb":
        from . import imdb
        return not imdb.missing_tables(conn, tables=["title"])   # each benchmark checks its own tables
    nsp, rel = SCHEMAS[name][1].split(".")
    return conn.execute("SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "WHERE n.nspname = %s AND c.relname = %s)", (nsp, rel)).fetchone()[0]


def load_schema(conn, name: str) -> None:
    """Create a data set. A SQL file runs statement by statement, because VACUUM cannot run in a transaction."""
    if name == "stats":
        from . import stats
        stats.load(conn)
        return
    if name == "imdb":
        from . import imdb
        imdb.require(conn, tables=["title"])   # raises imdb.NotLoaded; each benchmark checks its own tables
        return
    with open(SCHEMAS[name][0]) as f:
        sql = "\n".join(line for line in f if not line.lstrip().startswith("--"))
    for stmt in sql.split(";\n"):
        if stmt.strip():
            conn.execute(stmt)


def reset_log(conn) -> None:
    """Empty the observation tables, so that every run starts from a clean log."""
    conn.execute("TRUNCATE pgpi.plans, pgpi.stats")
    conn.execute("SELECT pgpi_stats_reset_seen()")


def sock_path(tag: str) -> str:
    """Return a socket path of its own for an estimator server. A stale path would keep old connections."""
    return f"/tmp/pgpi_bench_{tag}_{os.getpid()}.sock"


def prepare(conn, *schemas: str, reload: bool = False, say=print) -> None:
    """Load the data sets that a benchmark needs. Load all of them with reload, otherwise only the missing ones."""
    for name in schemas:
        if name == "imdb":                  # never loaded here: only checked
            load_schema(conn, name)
            continue
        if reload or not schema_loaded(conn, name):
            src = SCHEMAS[name][0]
            say(f"   loading {os.path.relpath(src, os.path.dirname(DEMO_DIR)) if src else name} ...")
            t0 = time.time()
            load_schema(conn, name)
            say(f"   loaded in {time.time() - t0:.0f}s")
