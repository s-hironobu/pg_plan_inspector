"""Dataset schema and the transformation of logged plans into table rows.

The dataset has four tables, stored as Parquet and read as Arrow:

plans     one row per executed (or canceled) statement
nodes     one row per plan node, with derived, ready-to-use columns
requests  intervention requests stored with the plans (pgpi.log_requests)
stats     statistics contents, one row per (dbid, relid, version)

Derived node columns follow one convention. So estimates and true values are
always comparable:

* parameterized points (``param_base``/``param_join``): rows per loop
  (per outer tuple), i.e. ``ntuples / nloops``;
* partial nodes (below a Gather, fed by a parallel-aware scan): total rows over
  all processes, i.e. ``ntuples``. They are compared with the estimate of the
  planner for the full relation;
* everything else: rows per loop.
"""

from __future__ import annotations

import json
import math
from typing import Any

import pyarrow as pa

from .request import Request, normalize_route

SCHEMA_VERSION = 1

STATS_REF = pa.struct([("relid", pa.int64()), ("rel", pa.string()), ("version", pa.int64())])

PLANS_SCHEMA = pa.schema([
    ("seqid", pa.int64()),
    ("logged_at", pa.timestamp("us", tz="UTC")),
    ("start_time", pa.timestamp("us", tz="UTC")),
    ("end_time", pa.timestamp("us", tz="UTC")),
    ("planning_ms", pa.float64()),
    ("execution_ms", pa.float64()),
    ("dbid", pa.int64()),
    ("database", pa.string()),
    ("username", pa.string()),
    ("pid", pa.int32()),
    ("nested_level", pa.int32()),
    ("queryid", pa.int64()),
    ("planid", pa.int64()),
    ("status", pa.string()),
    ("label", pa.string()),
    ("query", pa.string()),
    ("params", pa.string()),
    ("explain", pa.string()),
    ("mode", pa.string()),
    ("n_nodes", pa.int32()),
    ("stats_refs", pa.list_(STATS_REF)),
    ("meta", pa.string()),
])

NODES_SCHEMA = pa.schema([
    ("seqid", pa.int64()),
    ("node_id", pa.int32()),
    ("parent", pa.int32()),
    ("role", pa.string()),
    ("type", pa.string()),
    ("label", pa.string()),
    ("queryid", pa.int64()),
    ("planid", pa.int64()),
    ("point", pa.string()),
    ("relids", pa.list_(pa.int32())),
    ("ident", pa.list_(pa.string())),
    ("tables", pa.list_(pa.string())),
    ("n_rels", pa.int32()),
    ("param_outer", pa.list_(pa.int32())),
    ("plan_rows", pa.float64()),
    ("startup_cost", pa.float64()),
    ("total_cost", pa.float64()),
    ("width", pa.int32()),
    ("parallel_aware", pa.bool_()),
    ("partial", pa.bool_()),
    ("parallel_divisor", pa.float64()),
    ("ntuples", pa.float64()),
    ("nloops", pa.float64()),
    ("done", pa.float64()),
    ("worker_ntuples", pa.float64()),
    ("worker_loops", pa.float64()),
    ("nfiltered1", pa.float64()),
    ("nfiltered2", pa.float64()),
    ("censored", pa.bool_()),
    ("censor_basis", pa.string()),
    ("inner_unique_child", pa.bool_()),
    ("est_pg", pa.float64()),          # PostgreSQL's own estimate (same basis as true_rows)
    ("est_plan", pa.float64()),        # the value the planner used (same basis)
    ("est_answer", pa.float64()),      # the intervention's answer, if any
    ("est_source", pa.string()),
    ("est_applied", pa.bool_()),
    ("est_route", pa.string()),
    ("true_rows", pa.float64()),
    ("q_error_pg", pa.float64()),
    ("usable", pa.bool_()),            # CE node, descriptor present, executed, not censored
    ("ce_key", pa.string()),
    ("ce_template", pa.string()),
    ("ce", pa.string()),               # the descriptor, JSON (same format as a request)
    ("relinfo", pa.string()),          # JSON: pages, tuples, allvisfrac (base scans)
    ("index", pa.string()),            # JSON: index sizes (index scans)
])

REQUESTS_SCHEMA = pa.schema([
    ("seqid", pa.int64()),
    ("seq", pa.int64()),
    ("node_id", pa.int32()),
    ("point", pa.string()),
    ("route", pa.string()),
    ("pg", pa.float64()),
    ("answer", pa.float64()),
    ("source", pa.string()),
    ("applied", pa.bool_()),
    ("key", pa.string()),
    ("template", pa.string()),
    ("request", pa.string()),
])

STATS_SCHEMA = pa.schema([
    ("dbid", pa.int64()),
    ("database", pa.string()),
    ("relid", pa.int64()),
    ("rel", pa.string()),
    ("version", pa.int64()),
    ("captured_at", pa.timestamp("us", tz="UTC")),
    ("stats", pa.string()),
])

SCHEMAS = {"plans": PLANS_SCHEMA, "nodes": NODES_SCHEMA,
           "requests": REQUESTS_SCHEMA, "stats": STATS_SCHEMA}

JOIN_TYPES = ("Nested Loop", "Hash Join", "Merge Join")
GATHERS = ("Gather", "Gather Merge")


def q_error(est: float | None, true: float | None) -> float | None:
    """Return max(est/true, true/est), with both values clamped to at least 1 row."""
    if est is None or true is None or (isinstance(est, float) and math.isnan(est)):
        return None
    e = max(float(est), 1.0)
    t = max(float(true), 1.0)
    return max(e / t, t / e)


def parallel_divisor(workers: int, leader_participation: bool = True) -> float:
    """Return the same value as get_parallel_divisor() of PostgreSQL."""
    if workers <= 0:
        return 1.0
    d = float(workers)
    if leader_participation:
        lead = 1.0 - 0.3 * workers
        if lead > 0:
            d += lead
    return d


def _jdump(v: Any) -> str | None:
    return None if v is None else json.dumps(v, separators=(",", ":"))


def _mark_partial(nodes: list[dict]) -> None:
    """Compute 'partial' (rows split among parallel processes) and the divisors."""
    by_id = {n["id"]: n for n in nodes}
    children: dict[int, list[dict]] = {}
    for n in nodes:
        children.setdefault(n.get("parent", -1), []).append(n)

    def visit(n: dict, workers: int) -> bool:
        kids = children.get(n["id"], [])
        if n.get("type") in GATHERS:
            w = int(n.get("workers_planned", 0) or 0)
            for k in kids:
                visit(k, w if k.get("role") in ("outer",) else workers)
            n["_partial"] = False
            n["_workers"] = workers
            return False
        child_partial = {k["id"]: visit(k, workers) for k in kids}
        role_of = {k["id"]: k.get("role") for k in kids}
        if n.get("parallel_aware"):
            p = True
        elif n.get("type") in JOIN_TYPES:
            p = any(child_partial[i] for i in child_partial if role_of[i] == "outer")
        else:
            p = any(child_partial[i] for i in child_partial
                    if role_of[i] in ("outer", "member", "subquery"))
        p = p and workers > 0
        n["_partial"] = p
        n["_workers"] = workers
        return p

    for n in nodes:
        if n.get("parent", -1) == -1 or n.get("parent") not in by_id:
            visit(n, 0)


def plan_to_rows(rec: dict) -> tuple[dict, list[dict], list[dict]]:
    """Turn one pgpi.plans row (as a dict, JSON columns parsed) into table rows.

    ``rec`` has the columns of pgpi.plans as keys. ``nodes`` and ``meta`` are
    parsed JSON (lists or dicts) or None. ``explain`` is text.
    """
    meta = rec.get("meta") or {}
    nodes = rec.get("nodes") or []
    plan = {k: rec.get(k) for k in PLANS_SCHEMA.names if k not in ("meta", "stats_refs", "mode",
                                                                    "n_nodes")}
    plan["mode"] = meta.get("mode")
    plan["n_nodes"] = len(nodes)
    plan["stats_refs"] = [{"relid": r.get("relid"), "rel": r.get("rel"),
                           "version": r.get("version")} for r in meta.get("stats_refs", [])]
    plan["meta"] = _jdump(meta)

    _mark_partial(nodes)
    out_nodes = []
    for n in nodes:
        ce = n.get("ce")
        est = n.get("est") or {}
        point = n.get("point")
        ntuples = n.get("ntuples")
        nloops = n.get("nloops") or 0.0
        partial = bool(n.get("_partial"))
        divisor = parallel_divisor(int(n.get("_workers", 0) or 0)) if partial else 1.0
        plan_rows = n.get("plan_rows")
        is_param = point in ("param_base", "param_join")

        true_rows = None
        if nloops and ntuples is not None:
            if partial and not is_param:
                true_rows = float(ntuples)
            else:
                true_rows = float(ntuples) / float(nloops)

        if partial and not is_param:
            est_plan = (ce or {}).get("rel_rows") or (plan_rows * divisor if plan_rows is not None else None)
        else:
            est_plan = plan_rows
        est_pg = est.get("pg") if est.get("pg") is not None else est_plan

        censored = bool(n.get("censored")) if n.get("censored") is not None else False
        usable = bool(point and ce and nloops and not censored)
        req = Request(ce) if ce else None
        tables = list(req.tables) if req else None

        out_nodes.append({
            "seqid": rec.get("seqid"),
            "node_id": n.get("id"),
            "parent": n.get("parent"),
            "role": n.get("role"),
            "type": n.get("type"),
            "label": rec.get("label"),
            "queryid": rec.get("queryid"),
            "planid": rec.get("planid"),
            "point": point,
            "relids": n.get("relids"),
            "ident": n.get("ident"),
            "tables": tables,
            "n_rels": len(n.get("relids") or []),
            "param_outer": n.get("param_outer"),
            "plan_rows": plan_rows,
            "startup_cost": n.get("startup_cost"),
            "total_cost": n.get("total_cost"),
            "width": n.get("width"),
            "parallel_aware": n.get("parallel_aware"),
            "partial": partial,
            "parallel_divisor": divisor,
            "ntuples": ntuples,
            "nloops": n.get("nloops"),
            "done": n.get("done"),
            "worker_ntuples": n.get("worker_ntuples"),
            "worker_loops": n.get("worker_loops"),
            "nfiltered1": n.get("nfiltered1"),
            "nfiltered2": n.get("nfiltered2"),
            "censored": n.get("censored"),
            "censor_basis": n.get("censor_basis"),
            "inner_unique_child": bool(n.get("inner_unique_child", False)),
            "est_pg": est_pg if point else None,
            "est_plan": est_plan if point else None,
            "est_answer": est.get("answer"),
            "est_source": est.get("source"),
            "est_applied": est.get("applied"),
            "est_route": normalize_route(est.get("route")),
            "true_rows": true_rows,
            "q_error_pg": q_error(est_pg, true_rows) if usable else None,
            "usable": usable,
            "ce_key": req.key() if req else None,
            "ce_template": req.template() if req else None,
            "ce": _jdump(ce),
            "relinfo": _jdump(n.get("relinfo")),
            "index": _jdump(n.get("index")),
        })

    out_reqs = []
    for r in meta.get("requests", []):
        rq = r.get("request") or {}
        req = Request(rq)
        out_reqs.append({
            "seqid": rec.get("seqid"),
            "seq": r.get("seq"),
            "node_id": r.get("node"),
            "point": rq.get("point"),
            "route": normalize_route(rq.get("route")),
            "pg": rq.get("pg"),
            "answer": r.get("answer"),
            "source": r.get("source"),
            "applied": r.get("applied"),
            "key": req.key(),
            "template": req.template(),
            "request": _jdump(rq),
        })
    return plan, out_nodes, out_reqs
