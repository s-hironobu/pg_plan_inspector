"""Structured predicates from descriptors (shared by learned estimators).

The descriptors carry clauses as SQL produced by the deparser of pgpi, which has
a small, regular output. This module turns them into

* single-table predicates  (column, operator, literal, type), and
* equi-join keys            ((alias, column), (alias, column)),

and splits a question into per-alias table queries plus join-key groups. This is
the input of query-driven models such as GRASP and MSCN. Nothing here touches
table data.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass, field

from .request import JOIN_INNER, Request

OPS = ("=", "<>", "<", "<=", ">", ">=", "like", "notlike", "in", "isnull", "notnull", "other")
RANGE_OPS = frozenset(("<", "<=", ">", ">="))
FLIP = {"<": ">", "<=": ">=", ">": "<", ">=": "<=", "=": "=", "<>": "<>"}
SQLOPS = {"=": "=", "<>": "<>", "!=": "<>", "<": "<", "<=": "<=", ">": ">", ">=": ">=",
          "~~": "like", "!~~": "notlike", "~~*": "like", "!~~*": "notlike"}

NUMERIC_TYPES = ("smallint", "integer", "bigint", "numeric", "real", "double precision", "oid",
                 "date", "timestamp without time zone", "timestamp with time zone")

_ID = r'(?:"(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_$]*)'
_COL = rf'\(?(?P<{{n}}a>{_ID})\.(?P<{{n}}c>{_ID})\)?(?:::[a-z ]+(?:\(\d+(?:,\d+)?\))?)?'
_LIT = r"'(?P<lit>(?:[^']|'')*)'::(?P<type>[a-z ]+(?:\(\d+(?:,\d+)?\))?(?:\[\])?)"
_OP = r"(?P<op>=|<>|!=|<=|>=|<|>|~~\*|!~~\*|~~|!~~)"

RE_COL_OP_LIT = re.compile(rf"^\({_COL.format(n='x')} {_OP} {_LIT}\)$")
RE_LIT_OP_COL = re.compile(rf"^\({_LIT} {_OP} {_COL.format(n='x')}\)$")
RE_ANY = re.compile(rf"^\({_COL.format(n='x')} {_OP} (?P<q>ANY|ALL) \({_LIT}\)\)$")
RE_NULL = re.compile(rf"^\({_COL.format(n='x')} IS (?P<not>NOT )?NULL\)$")
RE_JOIN = re.compile(rf"^\({_COL.format(n='l')} = {_COL.format(n='r')}\)$")
RE_ANYCOL = re.compile(rf"(?P<a>{_ID})\.(?P<c>{_ID})")


def _unq(s: str) -> str:
    return s[1:-1].replace('""', '"') if s.startswith('"') else s


@dataclass(frozen=True)
class Pred:
    column: str | None      # None if no column could be identified
    op: str                 # one of OPS
    value: str | None       # literal text (for IN: the array literal)
    vtype: str | None       # PostgreSQL type of the literal
    sql: str

    @property
    def is_range(self) -> bool:
        return self.op in RANGE_OPS and self.number is not None

    @property
    def number(self) -> float | None:
        return to_number(self.value, self.vtype)


def to_number(value: str | None, vtype: str | None) -> float | None:
    """Return the numeric value of a literal of a numeric, date or timestamp type."""
    if value is None or vtype is None:
        return None
    t = vtype.split("(")[0].strip()
    if t not in NUMERIC_TYPES:
        return None
    try:
        if t == "date":
            return float(dt.date.fromisoformat(value).toordinal())
        if t.startswith("timestamp"):
            return dt.datetime.fromisoformat(value.replace(" ", "T")).timestamp()
        return float(value)
    except ValueError:
        return None


def parse_single(sql: str | None, alias: str) -> Pred:
    """A single-table clause. Unknown shapes become op 'other'."""
    if sql is None:
        return Pred(None, "other", None, None, "<opaque>")
    m = RE_COL_OP_LIT.match(sql)
    if m and _unq(m["xa"]) == alias:
        return Pred(_unq(m["xc"]), SQLOPS[m["op"]], m["lit"].replace("''", "'"), m["type"], sql)
    m = RE_LIT_OP_COL.match(sql)
    if m and _unq(m["xa"]) == alias:
        op = SQLOPS[m["op"]]
        return Pred(_unq(m["xc"]), FLIP.get(op, op), m["lit"].replace("''", "'"), m["type"], sql)
    m = RE_ANY.match(sql)
    if m and _unq(m["xa"]) == alias:
        op = "in" if (m["q"] == "ANY" and m["op"] == "=") else "other"
        return Pred(_unq(m["xc"]), op, m["lit"].replace("''", "'"), m["type"], sql)
    m = RE_NULL.match(sql)
    if m and _unq(m["xa"]) == alias:
        return Pred(_unq(m["xc"]), "notnull" if m["not"] else "isnull", None, None, sql)
    cols = [_unq(c["c"]) for c in RE_ANYCOL.finditer(sql) if _unq(c["a"]) == alias]
    return Pred(cols[0] if cols else None, "other", None, None, sql)


def parse_join(sql: str | None) -> tuple[tuple[str, str], tuple[str, str]] | None:
    """Return an equi-join clause between two columns, or None."""
    if sql is None:
        return None
    m = RE_JOIN.match(sql)
    if not m:
        return None
    return (_unq(m["la"]), _unq(m["lc"])), (_unq(m["ra"]), _unq(m["rc"]))


@dataclass(frozen=True)
class TableQuery:
    table: str
    preds: tuple[Pred, ...]


@dataclass
class Composite:
    """A question split into per-alias table queries and join-key groups.

    rows = prod_{targets} c(table query) * prod_{groups} overlap(group)

    For a parameterized question, the outer relations appear only through their
    join keys, because their own cardinality cancels out of rows per outer tuple.
    """
    targets: list[tuple[str, TableQuery]] = field(default_factory=list)
    outers: list[tuple[str, TableQuery]] = field(default_factory=list)
    groups: list[list[tuple[str, str]]] = field(default_factory=list)   # (alias, column)

    def aliases(self):
        return self.targets + self.outers


def decompose(req: Request) -> tuple[Composite | None, str | None]:
    """Split a question. Return (composite, None) or (None, reason to abstain)."""
    if req.point in ("join", "param_join") and req.jointype != JOIN_INNER:
        return None, "outer/semi/anti join"
    if req.opaque:
        return None, "clause not deparsed"
    alias_table: dict[str, str] = {}
    for r in req.rels + req.outer:
        if not r.rel:
            return None, "not a table"
        alias_table[r.alias] = r.rel
    single: dict[str, list[Pred]] = {a: [] for a in alias_table}
    edges = []
    for c in req.clauses + req.outer_clauses:
        if len(c.rels) == 1 and c.rels[0] in single:
            single[c.rels[0]].append(parse_single(c.sql, c.rels[0]))
        elif len(c.rels) == 2:
            j = parse_join(c.sql)
            if j is None or j[0][0] not in alias_table or j[1][0] not in alias_table:
                return None, "join clause that is not a column equality"
            edges.append(j)
        elif len(c.rels) == 0:
            continue
        else:
            return None, "clause over more than two relations"

    # key groups: union-find over (alias, column)
    parent: dict[tuple, tuple] = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in edges:
        parent[find(a)] = find(b)
    groups_map: dict[tuple, list] = {}
    for x in list(parent):
        groups_map.setdefault(find(x), []).append(x)
    targets = {r.alias for r in req.rels}
    groups = [sorted(g) for g in groups_map.values()
              if len(g) >= 2 and any(a in targets for a, _ in g)]

    comp = Composite()
    for r in req.rels:
        comp.targets.append((r.alias, TableQuery(r.rel, tuple(single[r.alias]))))
    for r in req.outer:
        comp.outers.append((r.alias, TableQuery(r.rel, tuple(single[r.alias]))))
    comp.groups = groups
    return comp, None
