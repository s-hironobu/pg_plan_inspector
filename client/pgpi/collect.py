"""Collector: pgpi.plans / pgpi.stats  ->  a Parquet dataset.

It is incremental. The manifest remembers the last seqid and the last statistics
capture time, so running it again appends only what is new.

    python3 -m pgpi collect --dsn "host=/tmp dbname=postgres" --out ds/ [--label 'exp-%']
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .schema import PLANS_SCHEMA, NODES_SCHEMA, REQUESTS_SCHEMA, SCHEMA_VERSION, STATS_SCHEMA, plan_to_rows

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None

MANIFEST = "manifest.json"
TABLES = ("plans", "nodes", "requests", "stats")


def _dsn_hint(dsn: str) -> str:
    """The DSN without secrets (for the manifest)."""
    return re.sub(r"password=\S+", "password=***", dsn)


def read_manifest(path: str) -> dict:
    p = os.path.join(path, MANIFEST)
    if os.path.exists(p):
        with open(p) as f:
            return json.load(f)
    return {"schema_version": SCHEMA_VERSION, "created": dt.datetime.now(dt.timezone.utc).isoformat(),
            "last_seqid": 0, "last_stats_at": None, "parts": 0, "filter": None,
            "anonymization": None, "sources": []}


def write_manifest(path: str, m: dict) -> None:
    tmp = os.path.join(path, MANIFEST + ".tmp")
    with open(tmp, "w") as f:
        json.dump(m, f, indent=2, default=str)
    os.replace(tmp, os.path.join(path, MANIFEST))


def write_part(path: str, table: str, rows: list[dict], part: int) -> int:
    if not rows:
        return 0
    os.makedirs(os.path.join(path, table), exist_ok=True)
    schema = {"plans": PLANS_SCHEMA, "nodes": NODES_SCHEMA, "requests": REQUESTS_SCHEMA, "stats": STATS_SCHEMA}[table]
    # JSON numbers arrive as int when they have no fraction. A float64 column refuses an int
    # beyond 2**53 (an estimate of 1.6e17 rows, say) unless it is converted first.
    floats = [f.name for f in schema if pa.types.is_floating(f.type)]
    for r in rows:
        for k in floats:
            v = r.get(k)
            if isinstance(v, int) and not isinstance(v, bool):
                r[k] = float(v)
    t = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(t, os.path.join(path, table, f"part-{part:06d}.parquet"), compression="zstd")
    return len(rows)


PLAN_COLUMNS = ("seqid, logged_at, start_time, end_time, planning_ms, execution_ms, dbid::bigint, "
                "database::text, username::text, pid, nested_level, queryid, planid, status, label, "
                "query, params, explain::text, nodes, meta")


def collect(dsn: str, out: str, *, label: str | None = None, database: str | None = None,
            batch: int = 2000, anonymizer=None, conn=None) -> dict[str, int]:
    """Append new plans and statistics to the dataset at ``out``."""
    if psycopg is None and conn is None:
        raise RuntimeError("collect requires psycopg")
    os.makedirs(out, exist_ok=True)
    m = read_manifest(out)
    filt = {"label": label, "database": database}
    if m.get("filter") is None and m["last_seqid"] == 0:
        m["filter"] = filt
    elif m.get("filter") != filt:
        raise ValueError(f"dataset {out} was collected with filter {m.get('filter')}, not {filt}")
    anon = anonymizer.describe() if anonymizer else None
    if m["last_seqid"] == 0 and m.get("anonymization") is None:
        m["anonymization"] = anon
    elif m.get("anonymization") != anon:
        raise ValueError("anonymization settings differ from the dataset's")

    own = conn is None
    if own:
        conn = psycopg.connect(dsn, autocommit=True)
    counts = {t: 0 for t in TABLES}
    try:
        where = ["seqid > %s"]
        args: list[Any] = []
        if label:
            where.append("label LIKE %s")
        if database:
            where.append("database = %s")
        while True:
            a = [m["last_seqid"]] + ([label] if label else []) + ([database] if database else [])
            cur = conn.execute(f"SELECT {PLAN_COLUMNS} FROM pgpi.plans WHERE {' AND '.join(where)} "
                               f"ORDER BY seqid LIMIT {int(batch)}", a)
            names = [d.name for d in cur.description]
            fetched = cur.fetchall()
            if not fetched:
                break
            plans, nodes, reqs = [], [], []
            for row in fetched:
                rec = dict(zip(names, row))
                if anonymizer:
                    rec = anonymizer.plan_record(rec)
                p, ns, rs = plan_to_rows(rec)
                plans.append(p)
                nodes.extend(ns)
                reqs.extend(rs)
            part = m["parts"] + 1
            counts["plans"] += write_part(out, "plans", plans, part)
            counts["nodes"] += write_part(out, "nodes", nodes, part)
            counts["requests"] += write_part(out, "requests", reqs, part)
            m["parts"] = part
            m["last_seqid"] = int(fetched[-1][0])
            write_manifest(out, m)

        # the statistics captured since the last run
        sw = "captured_at > %s" if m.get("last_stats_at") else "true"
        sa = [m["last_stats_at"]] if m.get("last_stats_at") else []
        if database:
            sw += " AND database = %s"
            sa.append(database)
        cur = conn.execute("SELECT dbid::bigint, database::text, relid::bigint, rel, version, "
                           f"captured_at, stats::text FROM pgpi.stats WHERE {sw} ORDER BY captured_at", sa)
        names = [d.name for d in cur.description]
        srows = [dict(zip(names, r)) for r in cur.fetchall()]
        if srows:
            if anonymizer:
                srows = [anonymizer.stats_record(r) for r in srows]
            part = m["parts"] + 1
            counts["stats"] += write_part(out, "stats", srows, part)
            m["parts"] = part
            m["last_stats_at"] = max(r["captured_at"] for r in srows).isoformat()
        m["sources"] = sorted(set(m.get("sources", [])) | {_dsn_hint(dsn)})
        m["updated"] = dt.datetime.now(dt.timezone.utc).isoformat()
        write_manifest(out, m)
    finally:
        if own:
            conn.close()
    return counts
