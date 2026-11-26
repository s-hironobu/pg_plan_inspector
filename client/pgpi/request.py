"""The cardinality question.

One type serves both sides of the framework:

* a live request sent by the pgpi extension while PostgreSQL plans a query,
* the descriptor (``ce``) of an executed plan node in the observation log.

Both carry the same fields, produced by the same C code. So a model trained on
observations can answer live requests without any feature translation.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Iterable

POINTS = ("base", "join", "param_base", "param_join", "clauselist")

# Intervention routes: H = planner hooks, D = detours of the size functions.
# Datasets collected before version 0.10 call them B and E.
ROUTES = ("H", "D")
_OLD_ROUTES = {"B": "H", "E": "D"}


def normalize_route(route: str | None) -> str | None:
    """Return the route letter of a request. The old names (B, E) map to H and D."""
    return _OLD_ROUTES.get(route, route)
PARAM_POINTS = ("param_base", "param_join")

# the PostgreSQL JoinType values used here
JOIN_INNER = 0

_LITERAL = re.compile(r"'(?:[^']|'')*'")


def strip_literals(sql: str) -> str:
    """Replace every quoted literal by '?' (the deparser quotes all constants)."""
    return _LITERAL.sub("'?'", sql)


@dataclass(frozen=True)
class Rel:
    alias: str
    rel: str | None          # schema-qualified, quoted like PostgreSQL does; None if not a table
    relid: int | None = None
    rti: int | None = None
    inh: bool = False
    rtekind: int | None = None

    @classmethod
    def from_json(cls, d: dict) -> "Rel":
        return cls(alias=d.get("alias"), rel=d.get("rel"), relid=d.get("relid"),
                   rti=d.get("rti"), inh=bool(d.get("inh", False)),
                   rtekind=d.get("rtekind"))

    def sql_from_item(self) -> str:
        alias = '"' + self.alias.replace('"', '""') + '"'
        return f"{self.rel} AS {alias}"


@dataclass(frozen=True)
class Clause:
    sql: str | None          # executable SQL, or None if the clause could not be deparsed
    rels: tuple[str, ...]    # aliases it references

    @classmethod
    def from_json(cls, d: dict) -> "Clause":
        rels = d.get("rels") or ""
        return cls(sql=d.get("sql"), rels=tuple(rels.split()) if rels else ())


class Request:
    """A cardinality question (live request or observed node descriptor).

    Attributes mirror the JSON fields. ``data`` keeps the original dict.
    ``pg`` is the estimate of PostgreSQL (rows, or selectivity for
    ``clauselist``). It is None for a descriptor without a matching request.
    """

    __slots__ = ("data", "point", "rels", "outer", "clauses", "outer_clauses",
                 "opaque", "nonrel", "jointype", "pg", "tuples", "label",
                 "queryid", "plan_name", "route", "_key", "_template")

    def __init__(self, data: dict[str, Any]):
        self.data = data
        self.point: str = data.get("point", "base")
        self.rels = tuple(Rel.from_json(r) for r in data.get("rels", []))
        self.outer = tuple(Rel.from_json(r) for r in data.get("outer", []))
        self.clauses = tuple(Clause.from_json(c) for c in data.get("clauses", []))
        self.outer_clauses = tuple(Clause.from_json(c) for c in data.get("outer_clauses", []))
        self.opaque: int = int(data.get("opaque", 0) or 0)
        self.nonrel: bool = bool(data.get("nonrel", False))
        self.jointype: int = int(data.get("jointype", 0) or 0)
        self.pg: float | None = data.get("pg")
        self.tuples: float | None = data.get("tuples")
        self.label: str | None = data.get("label")
        self.queryid: int | None = data.get("queryid")
        self.plan_name: str | None = data.get("plan_name")
        self.route: str | None = normalize_route(data.get("route"))
        self._key = None
        self._template = None

    # ----------------------------------------------------------- basics

    @classmethod
    def from_json(cls, text: str | bytes) -> "Request":
        return cls(json.loads(text))

    def __repr__(self) -> str:
        o = f" | {' '.join(self.outer_aliases)}" if self.outer else ""
        return f"<Request {self.point} ({' '.join(self.aliases)}{o}) pg={self.pg}>"

    @property
    def is_param(self) -> bool:
        """Return True if rows are per outer tuple (parameterized scan, or clause list with outer relations)."""
        return self.point in PARAM_POINTS or (self.point == "clauselist" and bool(self.outer))

    @property
    def aliases(self) -> tuple[str, ...]:
        return tuple(sorted(r.alias for r in self.rels))

    @property
    def outer_aliases(self) -> tuple[str, ...]:
        return tuple(sorted(r.alias for r in self.outer))

    @property
    def tables(self) -> tuple[str, ...]:
        """Return the table names (sorted, with repeats for self-joins)."""
        return tuple(sorted(r.rel or f"<{r.alias}>" for r in self.rels))

    @property
    def n_rels(self) -> int:
        return len(self.rels)

    @property
    def complete(self) -> bool:
        """Return True if all clauses are deparsed and every relation is a table, so the SQL can be rebuilt."""
        return self.opaque == 0 and not self.nonrel and all(r.rel for r in self.rels + self.outer)

    # ----------------------------------------------------------- identity

    def _canonical(self, literals: bool) -> str:
        """Return the canonical text. It does not depend on RTIs, clause order or route."""
        point = self.point
        if point == "clauselist":
            point = "param_base" if self.outer else "base"

        def rels(rs: Iterable[Rel]):
            return sorted([r.rel or f"<{r.rtekind}>", r.alias] for r in rs)

        def clauses(cs: Iterable[Clause]):
            out = []
            for c in cs:
                s = c.sql if c.sql is not None else "<opaque>"
                out.append(s if literals else strip_literals(s))
            return sorted(out)

        return json.dumps([point, rels(self.rels), rels(self.outer),
                           clauses(self.clauses), clauses(self.outer_clauses),
                           self.jointype], separators=(",", ":"))

    def key(self) -> str:
        """Return the identity of the question, literals included (for exact lookups)."""
        if self._key is None:
            self._key = hashlib.sha1(self._canonical(True).encode()).hexdigest()[:20]
        return self._key

    def template(self) -> str:
        """Return the identity with literals replaced by '?' (a query-class key)."""
        if self._template is None:
            self._template = hashlib.sha1(self._canonical(False).encode()).hexdigest()[:20]
        return self._template

    # ----------------------------------------------------------- SQL

    def count_sql(self) -> str | None:
        """Return SELECT count(*) over the target relations under their clauses."""
        if not self.complete:
            return None
        sql = "SELECT count(*) FROM " + ", ".join(r.sql_from_item() for r in self.rels)
        if self.clauses:
            sql += " WHERE " + " AND ".join(c.sql for c in self.clauses)
        return sql

    def param_sqls(self) -> tuple[str, list[str]] | None:
        """For a parameterized question, return (numerator, [denominators]).

        PostgreSQL estimates a parameterized size as rows per combination of
        outer tuples, i.e. |T join (O1 x O2 ...)| / (|O1| * |O2| ...). Each
        outer relation is under its own restrictions. This method returns the
        SQL for the numerator and for the count of each outer relation.
        """
        if not self.complete or not self.outer:
            return None
        by_alias: dict[str, list[str]] = {r.alias: [] for r in self.outer}
        for c in self.outer_clauses:
            if len(c.rels) == 1 and c.rels[0] in by_alias:
                by_alias[c.rels[0]].append(c.sql)
            else:
                return None
        allrels = self.rels + self.outer
        where = [c.sql for c in self.clauses] + [c.sql for c in self.outer_clauses]
        num = "SELECT count(*) FROM " + ", ".join(r.sql_from_item() for r in allrels)
        if where:
            num += " WHERE " + " AND ".join(where)
        dens = []
        for r in self.outer:
            d = "SELECT count(*) FROM " + r.sql_from_item()
            if by_alias[r.alias]:
                d += " WHERE " + " AND ".join(by_alias[r.alias])
            dens.append(d)
        return num, dens


# ---------------------------------------------------------------- answers

def rows(n: float) -> dict:
    """Answer with an absolute row count (per loop for parameterized points)."""
    return {"rows": float(n)}


def factor(f: float) -> dict:
    """Answer by scaling the estimate of PostgreSQL."""
    return {"factor": float(f)}


def sel(s: float) -> dict:
    """Answer with a selectivity (clauselist only)."""
    return {"sel": float(s)}


def normalize_answer(ans: Any) -> dict | None:
    """Accept None, a number (rows), or a dict with rows/factor/sel."""
    if ans is None:
        return None
    if isinstance(ans, (int, float)):
        return rows(ans)
    if isinstance(ans, dict):
        out = {k: float(ans[k]) for k in ("rows", "factor", "sel") if k in ans and ans[k] is not None}
        if not out:
            return None
        if "source" in ans:
            out["source"] = str(ans["source"])
        return out
    raise TypeError(f"an estimator returned {type(ans).__name__}; expected None, a number or a dict")


def answer_rows(req: Request, ans: dict | None) -> float | None:
    """Return the row count an answer stands for (rows per loop for param points).

    For ``clauselist`` requests this is selectivity * tuples.
    """
    if not ans:
        return None
    if "rows" in ans:
        return ans["rows"]
    if "factor" in ans:
        if req.pg is None:
            return None
        base = req.pg * (req.tuples or 0) if req.point == "clauselist" else req.pg
        return base * ans["factor"]
    if "sel" in ans and req.tuples:
        return ans["sel"] * req.tuples
    return None
