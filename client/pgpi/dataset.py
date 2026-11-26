"""Reading a dataset.

    ds = pgpi.Dataset("ds/")
    nodes = ds.nodes()                         # pyarrow.Table
    df = ds.nodes().to_pandas()                # or pandas
    con = ds.duckdb()                          # or SQL (duckdb, optional)
    for ex in ds.examples(points=["join"]):    # training examples
        ex.request, ex.true_rows, ex.est_pg
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Iterable, Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads

from .collect import read_manifest
from .request import Request
from .schema import SCHEMAS


@dataclass
class Example:
    """One observation usable for training or evaluation."""
    request: Request          # the descriptor (the same type as a live request)
    true_rows: float          # observed, on the basis of the request (per loop for param points)
    est_pg: float | None      # PostgreSQL's own estimate on the same basis
    est_plan: float | None    # what the planner used
    seqid: int
    node_id: int
    label: str | None
    queryid: int | None
    planid: int | None
    censored: bool = False    # the node stopped early: true_rows is a lower bound
    nloops: float = 1.0


class Dataset:
    def __init__(self, path: str):
        if not os.path.isdir(path):
            raise FileNotFoundError(path)
        self.path = path
        self.manifest = read_manifest(path)

    def _table(self, name: str, columns=None, filter=None) -> pa.Table:
        d = os.path.join(self.path, name)
        schema = SCHEMAS[name]
        if not os.path.isdir(d) or not os.listdir(d):
            t = schema.empty_table()
            return t.select(columns) if columns else t
        ds = pads.dataset(d, format="parquet", schema=schema)
        return ds.to_table(columns=columns, filter=filter)

    def plans(self, columns=None, filter=None) -> pa.Table:
        return self._table("plans", columns, filter)

    def nodes(self, columns=None, filter=None) -> pa.Table:
        return self._table("nodes", columns, filter)

    def requests(self, columns=None, filter=None) -> pa.Table:
        return self._table("requests", columns, filter)

    def stats(self, columns=None, filter=None) -> pa.Table:
        """Statistics contents (deduplicated by dbid, relid, version)."""
        t = self._table("stats", columns, filter)
        if t.num_rows == 0 or not {"dbid", "relid", "version"} <= set(t.column_names):
            return t
        seen, keep = set(), []
        for i, k in enumerate(zip(t["dbid"].to_pylist(), t["relid"].to_pylist(),
                                  t["version"].to_pylist())):
            if k not in seen:
                seen.add(k)
                keep.append(i)
        return t.take(keep)

    def stats_of(self, dbid: int, relid: int, version: int) -> dict | None:
        f = (pc.field("dbid") == dbid) & (pc.field("relid") == relid) & (pc.field("version") == version)
        t = self._table("stats", ["stats"], f)
        return json.loads(t["stats"][0].as_py()) if t.num_rows else None

    def duckdb(self):
        """Return a DuckDB connection with the views plans, nodes, requests and stats."""
        import duckdb
        con = duckdb.connect()
        for name in SCHEMAS:
            d = os.path.join(self.path, name)
            if os.path.isdir(d) and os.listdir(d):
                con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{d}/*.parquet')")
        return con

    def examples(self, points: Iterable[str] | None = None, *, labels: Iterable[str] | None = None,
                 usable_only: bool = True, filter=None) -> Iterator[Example]:
        """Yield training/evaluation examples from executed plan nodes.

        By default, only *usable* nodes: a cardinality node with a descriptor,
        executed at least once and not censored.
        """
        f = None
        if usable_only:
            f = pc.field("usable") == True  # noqa: E712
        if points:
            pf = pc.field("point").isin(list(points))
            f = pf if f is None else f & pf
        if labels:
            lf = pc.field("label").isin(list(labels))
            f = lf if f is None else f & lf
        if filter is not None:
            f = filter if f is None else f & filter
        if not usable_only:
            cf = pc.field("point").is_valid() & (pc.field("nloops") > 0)
            f = cf if f is None else f & cf
        cols = ["seqid", "node_id", "label", "queryid", "planid", "true_rows", "est_pg",
                "est_plan", "ce", "censored", "nloops"]
        t = self.nodes(cols, f)
        for row in t.to_pylist():
            if row["ce"] is None or row["true_rows"] is None:
                continue
            req = Request(json.loads(row["ce"]))
            req.pg = row["est_pg"]
            req.queryid = row["queryid"]          # live requests carry it too
            yield Example(request=req, true_rows=row["true_rows"], est_pg=row["est_pg"],
                          est_plan=row["est_plan"], seqid=row["seqid"], node_id=row["node_id"],
                          label=row["label"], queryid=row["queryid"], planid=row["planid"],
                          censored=bool(row["censored"]), nloops=row["nloops"] or 0.0)

    def summary(self) -> dict:
        p = self.plans(["seqid", "label", "status"])
        n = self.nodes(["point", "usable"])
        labels: dict[str, int] = {}
        for lab in p["label"].to_pylist():
            labels[lab or ""] = labels.get(lab or "", 0) + 1
        points: dict[str, list[int]] = {}
        for pt, u in zip(n["point"].to_pylist(), n["usable"].to_pylist()):
            if pt:
                c = points.setdefault(pt, [0, 0])
                c[0] += 1
                c[1] += 1 if u else 0
        return {"plans": p.num_rows, "nodes": n.num_rows, "labels": labels,
                "points": {k: {"nodes": v[0], "usable": v[1]} for k, v in sorted(points.items())},
                "stats": self.stats(["dbid", "relid", "version"]).num_rows,
                "anonymization": self.manifest.get("anonymization")}
