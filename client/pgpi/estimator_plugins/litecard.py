"""LiteCard: many small local regressors, one per recurring subquery pattern.

An implementation, as a pgpi plugin, of

    Z. Yi, S. Abu-el-Haija, Y. Wang, T. Vemparala, Y. Chronis, Y. Gan, M. Burrows,
    B. Perozzi, F. Ozcan, C. Binnig, R. Marcus, "Is it Bigger than a Breadbox:
    Efficient Cardinality Estimation for Real World Workloads", 2025
    (https://arxiv.org/abs/2510.03386).

It is written from the paper. No code of the authors was found. Where the paper leaves a
choice open, the choice made here is marked "not in the paper" below.

The method, as implemented here
-------------------------------

1. The subquery as a graph (Section 3.1, Appendix A). The predicates of the subquery are
   parsed into a parse tree (with sqlglot, the parser the paper names) and merged with
   the tables they refer to. The nodes have the types table, alias, column, op, literal,
   and, or, not, in_list and function. The edges go table -> alias -> column, and from
   each node of the parse tree to its children. Identical tables and columns are one
   node. The alias name is ignored (footnote 6). The subquery itself is a root node of
   type scan or join (the paper's graphs of PostgreSQL's RelOptInfos have these node
   types), with an edge to every alias and to the conjunction of its predicates.

2. Patterns (Section 3.2, Table 1, Eqs. 8-11). Every node gets a hash of its attributes
   in the pattern feature set H, and of its type, in-degree and out-degree (Appendix C).
   Then one pass in topological order adds the hashes of its parents, and one pass in
   reverse order adds the hashes of its children (MD5, as in Appendix C). The pattern
   hash is the hash of all node hashes in sorted order. This is also the canonical order
   of the nodes. There are three levels:

       H1  {(table, name), (column, type)}
       H2  H1 + {(column, name)}
       H3  H2 + {(op, code)}                         (only the literals differ)

3. Features (Eq. 12, Table 1, Appendix D). The features of the nodes, in the canonical
   order, for the learning feature set F of the level:

       F3  {(literal, value)}                        with H3
       F2  F3 + {(op, code)}                         with H2
       F1  F2 + {(column, numUniques)}               with H1

   The extractors are those of the paper:
   * A numeric literal compared with a column whose minimum and maximum are known is f3
     ([0, f2], [f2, 1] or [f2, f2] for <, > and =; f2 is the value min-max scaled by the
     range of the column). Other numeric literals are f2, or f1 when the range is
     unknown.
   * Dates are f5 (year, month, day).
   * Strings are f4 (the codes of the first three characters).
   * Operators are f8 (the 3-bit code of =, >, >=, <, <=).
   * numUniques is the number of distinct values of the column.
   The range and the distinct count of a column come from PostgreSQL's statistics as
   captured in the dataset (pg_statistic), as the paper takes them from the statistics of
   the engine.

4. Learning (Section 3.4). Every observed subquery (executed plan node) is filed, with its
   features and its actual row count, under its pattern at each level.

5. Answering (Section 2.2, Eqs. 5-7; Section 4). A question is answered at the most
   specific level whose bucket has enough history (beta_3 = 10, beta_2 = 50,
   beta_1 = 100). The answer comes from a regressor fitted at the time of the question on
   that bucket. Each observation is weighted by the Gaussian kernel
   exp(-||x - z||^2 / sigma^2) between its features and those of the question (Eqs. 5, 6).
   The regressor is gradient-boosted trees of Yggdrasil Decision Forests, the learner of
   the main evaluation of the paper (Appendix E.2), on the row count itself (Eq. 5).
   Without such a bucket, the plugin abstains (PostgreSQL's estimate stands). For joins,
   the estimate is corrected as in Appendix B.4: with the probability that PostgreSQL
   underestimates a subquery with that many joins, PostgreSQL's estimate of the subquery
   is multiplied by their average q-error (Table 5).

How this implementation differs from the paper (see also README.md in this directory)
------------------------------------------------------------------------------------

* The graph is built from the clauses PostgreSQL hands to pgpi, deparsed into SQL, not
  from the query text. The casts PostgreSQL adds to literals and columns are removed
  again. IN lists arrive as ``= ANY ('{...}')`` and are turned back into an in node with
  an in_list. BETWEEN arrives as two comparisons.
* Not in the paper (the paper does not say): the node types and edges of the root (scan /
  join, the role of an alias as the outer relation of a parameterized question, the join
  type); the code of operators other than the five ordinal ones (their name in the hash,
  000 as f8); how ties in the canonical order are broken (by the features); how
  timestamps are encoded (as dates, f5); numUniques as a count.
* Not given in the paper: the kernel width sigma, and the settings of the trees. Here the
  defaults of Yggdrasil are used, with one thread. The paper mentions only a height
  restriction. Where ydf is not available (Linux on ARM without a build of ydf), the
  trees are those of scikit-learn with the same settings (``GBDT_LIBRARY``), not those of
  Yggdrasil. Sigma has no meaning on features of very different scales (years, codes of
  characters, counts). So the features are min-max scaled over the bucket before the
  kernel is computed (``SCALE_FEATURES``; not in the paper). The kernel weights are
  divided by the largest one, which changes neither Eq. 5 nor Eq. 7.
* Table 5 is the one of the paper, measured on its IMDb workload, not on the database at
  hand.
* The paper injects the estimate of every subquery, so the correction multiplies
  PostgreSQL's own estimate of that subquery. In the planner, a join is estimated from its
  inputs as already answered. The plugin recovers PostgreSQL's estimate with uncorrected
  inputs from the earlier requests of the same planning, and corrects that. So the
  corrections of the levels below are not multiplied again (``_own_estimate``). A join
  whose inputs it has not seen is not corrected. Its rows are read as in the worked
  example of Appendix B.4 (3 joins: 1361.38).
* Learning is not online. fit() files the observations of the dataset (collect and restart
  the estimation server to add new ones). The regressor is fitted for each question, as
  in the paper, but its answer is kept for a repeated question.
* The table size (f6) and the column range (f7) are not used as features, because Table 1
  does not list them (``EXTRA_FEATURES``).
* Training data, as for all pgpi plugins: the nodes of executed plans, not every subquery
  of every query.

The switches below choose between the behaviour of the paper and alternatives. They are
constants of this file on purpose, not options. Requires ``sqlglot``. For MODEL = "gbdt" it
also needs ``ydf`` (Yggdrasil Decision Forests) or ``scikit-learn``:
pip install sqlglot scikit-learn [ydf].
"""
from __future__ import annotations

import datetime as dt
import functools
import hashlib
import json
import random

import numpy as np

from ..columns import StatsCatalog
from ..estimator import Estimator, register
from ..predicates import NUMERIC_TYPES, to_number
from ..request import JOIN_INNER, Request, strip_literals

try:
    import sqlglot
    from sqlglot import exp
except ImportError:                       # checked when the estimator is created
    sqlglot = None

# ---------------------------------------------------------------- switches

# The local regressor (Section 2.2).
# "gbdt": gradient-boosted trees (g_DF), the learner of the main evaluation of the paper.
# "rbf": the kernel-weighted average of the row counts of the neighbours (g_RBF, Eq. 7).
# "lr": locally weighted linear regression (g_LR).
MODEL = "gbdt"

# Regression target. The paper minimizes the squared error of the row count itself
# (Eq. 5). False: the row count, as in the paper. True: log(1 + rows).
TARGET_LOG = False

# Bias correction of Appendix B.4, for joins without a learned estimate. True: as in
# the paper. False: abstain, so PostgreSQL's estimate stands.
BIAS_CORRECTION = True
# Table 5 of the paper: number of joins -> (proportion underestimated, average q-error)
BIAS_TABLE = {1: (0.57, 1.57), 2: (0.83, 20.20), 3: (0.93, 1361.38), 4: (0.98, 68655.97)}
# True: a uniform random number decides, as in the paper (seeded, so runs are repeatable).
# False: a number derived from the question, so the same question gets the same answer.
BIAS_RANDOM = True
BIAS_SEED = 0
# How many of PostgreSQL's own estimates are kept for the bias correction of the joins
# above (see _own_estimate). The memory is emptied when it holds more.
NATIVE_MAX = 200_000

# Observations a bucket needs before its level is trusted (Section 4).
BETA = {3: 10, 2: 50, 1: 100}

# Not given in the paper.
SIGMA = 0.1              # width of the Gaussian kernel (on scaled features, see SCALE_FEATURES)
SCALE_FEATURES = True    # min-max scale the features over the bucket before the kernel
RIDGE = 1e-3             # l2 regularization of g_LR ("we use l2 regularization", no value given)
# The library of the trees.
# "ydf": Yggdrasil Decision Forests, as in the paper (no wheels for Linux on ARM).
# "sklearn": GradientBoostingRegressor of scikit-learn, with the defaults of Yggdrasil (below).
# "auto": ydf if it is installed, else sklearn.
GBDT_LIBRARY = "auto"
# Settings of ydf.GradientBoostedTreesLearner. Otherwise the defaults of Yggdrasil apply
# (300 trees at most, shrinkage 0.1, depth 6, 5 examples per leaf, early stopping on 10%
# of the bucket after 30 trees without improvement). One thread, as in the runs of the
# paper on a single CPU (and thread start-up costs more than training a small bucket).
GBDT = {"num_threads": 1}
# The same defaults for scikit-learn.
GBDT_SKLEARN = {"n_estimators": 300, "learning_rate": 0.1, "max_depth": 6, "min_samples_leaf": 5,
                "validation_fraction": 0.1, "n_iter_no_change": 30, "random_state": 0}
EXTRA_FEATURES = False   # also f6 (table size) and f7 (column range): not in Table 1

POINTS = ("base", "join", "param_base", "param_join")

# ---------------------------------------------------------------- the graph

ORDINAL = {"=": (0, 1, 0), ">": (0, 0, 1), ">=": (0, 1, 1), "<": (1, 0, 0), "<=": (1, 1, 0)}   # f8
COMPARE = {exp.EQ: "=", exp.NEQ: "<>", exp.GT: ">", exp.GTE: ">=", exp.LT: "<", exp.LTE: "<=",
           exp.Like: "like", exp.ILike: "ilike"} if sqlglot else {}
DATE_TYPES = ("date", "timestamp")


class Graph:
    """The nodes (type, attributes) and the directed edges of one subquery."""

    def __init__(self):
        self.type: list[str] = []
        self.attr: list[dict] = []
        self.out: list[list[int]] = []
        self.inn: list[list[int]] = []
        self._merged: dict = {}

    def node(self, type_: str, merge=None, **attr) -> int:
        if merge is not None and (type_, merge) in self._merged:
            return self._merged[(type_, merge)]
        self.type.append(type_)
        self.attr.append(attr)
        self.out.append([])
        self.inn.append([])
        i = len(self.type) - 1
        if merge is not None:
            self._merged[(type_, merge)] = i
        return i

    def edge(self, a: int, b: int) -> None:
        self.out[a].append(b)
        self.inn[b].append(a)

    def topological(self) -> list[int]:
        deg = [len(x) for x in self.inn]
        todo = [i for i, d in enumerate(deg) if d == 0]
        order = []
        while todo:
            i = todo.pop()
            order.append(i)
            for j in self.out[i]:
                deg[j] -= 1
                if deg[j] == 0:
                    todo.append(j)
        return order if len(order) == len(self.type) else list(range(len(self.type)))


@functools.lru_cache(maxsize=200000)
def _parse(sql: str):
    try:
        return sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return None


def _unwrap(e):
    while isinstance(e, exp.Paren):
        e = e.this
    return e


def _literal(e):
    """Return (text, type) of a literal, possibly cast; None if e is not one."""
    e = _unwrap(e)
    typ = None
    if isinstance(e, exp.Cast):
        typ = e.to.sql(dialect="postgres").lower()
        e = _unwrap(e.this)
    if isinstance(e, exp.Literal):
        return e.this, typ or ("text" if e.is_string else "numeric")
    if isinstance(e, exp.Neg) and isinstance(_unwrap(e.this), exp.Literal):
        return "-" + _unwrap(e.this).this, typ or "numeric"
    return None


def _column(e):
    """Return (alias, column) of a column reference, possibly cast; None otherwise."""
    e = _unwrap(e)
    if isinstance(e, exp.Cast):
        e = _unwrap(e.this)
    if isinstance(e, exp.Column) and e.table:
        return e.table, e.name
    return None


def _array_items(text: str) -> list[str]:
    s = text.strip()
    if s.startswith("{") and s.endswith("}"):
        s = s[1:-1]
    out, cur, q = [], "", False
    for ch in s:
        if ch == '"':
            q = not q
        elif ch == "," and not q:
            out.append(cur)
            cur = ""
        else:
            cur += ch
    if cur or s:
        out.append(cur)
    return [x.strip() for x in out]


class Builder:
    """Turn a question into its graph (step 1 of the module docstring)."""

    def __init__(self, req: Request, stats: "Stats"):
        self.g = Graph()
        self.stats = stats
        self.table = {r.alias: r.rel or f"<{r.rtekind}>" for r in req.rels + req.outer}
        self.alias_node: dict[str, int] = {}
        outer = {r.alias for r in req.outer}
        kind = "scan" if req.n_rels == 1 else "join"
        self.root = self.g.node(kind, structure=[req.point, req.jointype])
        for alias, rel in self.table.items():
            a = self.alias_node[alias] = self.g.node("alias", role="outer" if alias in outer else "target")
            self.g.edge(self.g.node("table", merge=rel, name=rel,
                                    size=stats.reltuples(rel)), a)
            self.g.edge(self.root, a)
        if req.clauses or req.outer_clauses:
            conj = self.g.node("and")
            self.g.edge(self.root, conj)
            for c in req.clauses + req.outer_clauses:
                self.g.edge(conj, self.clause(c.sql))

    def column(self, alias: str, col: str) -> int:
        rel = self.table.get(alias, "?")
        st = self.stats.column(rel, col)
        i = self.g.node("column", merge=(alias, col), name=col, type=st["type"], uniques=st["uniques"],
                        range=st["range"])
        if alias in self.alias_node and not self.g.inn[i]:
            self.g.edge(self.alias_node[alias], i)
        return i

    def literal(self, text: str, typ: str, op: str | None = None, col: int | None = None) -> int:
        return self.g.node("literal", value=text, vtype=typ, op=op, col=col)

    def clause(self, sql: str | None) -> int:
        e = _parse(sql) if sql else None
        if e is None:                                   # not parsed: one opaque node
            return self.g.node("function", name=strip_literals(sql or "<opaque>"))
        return self.expr(e)

    def expr(self, e) -> int:
        e = _unwrap(e)
        g = self.g
        if isinstance(e, (exp.And, exp.Or)):
            i = g.node("and" if isinstance(e, exp.And) else "or")
            for x in (e.this, e.expression):
                g.edge(i, self.expr(x))
            return i
        if isinstance(e, exp.Not):
            i = g.node("not")
            g.edge(i, self.expr(e.this))
            return i
        if isinstance(e, exp.Is):                       # IS [NOT] NULL
            i = g.node("op", code="is_null")
            g.edge(i, self.operand(e.this))
            return i
        if type(e) in COMPARE:
            op = COMPARE[type(e)]
            rhs = _unwrap(e.expression)
            quant = None
            if isinstance(rhs, exp.Any):
                quant, rhs = "any", rhs.this
            elif isinstance(rhs, exp.All):
                quant, rhs = "all", rhs.this
            elif isinstance(rhs, exp.Anonymous) and str(rhs.this).upper() in ("ANY", "ALL") and rhs.expressions:
                quant, rhs = str(rhs.this).lower(), rhs.expressions[0]
            if quant:                                   # x op ANY/ALL (array): IN, NOT IN, ...
                code = {("=", "any"): "in", ("<>", "all"): "not_in"}.get((op, quant), f"{op}_{quant}")
                i = g.node("op", code=code)
                left = self.operand(e.this)
                g.edge(i, left)
                lit = _literal(rhs)
                if lit is None:
                    g.edge(i, self.expr(rhs))
                    return i
                lst = g.node("in_list")
                g.edge(i, lst)
                typ = lit[1][:-2] if lit[1].endswith("[]") else lit[1]
                for item in _array_items(lit[0]):
                    g.edge(lst, self.literal(item, typ, None, left))
                return i
            i = g.node("op", code=op)
            a, b = e.this, e.expression
            if _literal(a) is not None and _literal(b) is None:     # literal op column: flip
                a, b = b, a
                op = {"<": ">", "<=": ">=", ">": "<", ">=": "<="}.get(op, op)
                g.attr[i]["code"] = op
            left = self.operand(a)
            g.edge(i, left)
            lit = _literal(b)
            g.edge(i, self.literal(lit[0], lit[1], op, left) if lit is not None else self.operand(b))
            return i
        return self.operand(e)

    def operand(self, e) -> int:
        e = _unwrap(e)
        col = _column(e)
        if col is not None:
            return self.column(*col)
        lit = _literal(e)
        if lit is not None:
            return self.literal(*lit)
        i = self.g.node("function", name=type(e).__name__.lower() if not isinstance(e, exp.Anonymous)
                        else str(e.this).lower())
        for x in e.iter_expressions():
            self.g.edge(i, self.expr(x))
        return i


# ---------------------------------------------------------------- statistics

class Stats:
    """Column types, ranges and distinct counts, taken from the captured pg_statistic."""

    def __init__(self, catalog: StatsCatalog | None):
        self.catalog = catalog
        self._cache: dict = {}

    def reltuples(self, rel: str) -> float:
        st = self.catalog.by_rel.get(rel) if self.catalog else None
        return float(st.get("reltuples") or 0.0) if st else 0.0

    def column(self, rel: str, col: str) -> dict:
        if (rel, col) in self._cache:
            return self._cache[(rel, col)]
        out = {"type": "-", "uniques": 0.0, "range": None}
        st = self.catalog.by_rel.get(rel) if self.catalog else None
        c = next((c for c in (st or {}).get("columns", []) if c.get("attname") == col and not c.get("inh")), None)
        if c is not None:
            typ = (c.get("type") or "-").split("(")[0].strip()
            nd = float(c.get("n_distinct") or 0.0)
            if nd < 0:
                nd = -nd * self.reltuples(rel)
            vals = []
            if typ in NUMERIC_TYPES and not typ.startswith(DATE_TYPES):
                for s in c.get("slots", []):
                    if s.get("kind") in (1, 2):                 # MCV, histogram
                        vals += [v for v in (to_number(x, typ) for x in s.get("values", [])) if v is not None]
            out = {"type": typ, "uniques": nd, "range": (min(vals), max(vals)) if vals else None}
        self._cache[(rel, col)] = out
        return out


# ---------------------------------------------------------------- patterns and features

# The attributes of each node type that are hashed at each level (Table 1). Besides
# these, the type, in-degree and out-degree of every node are hashed (Appendix C),
# and so are the structure of the root, the role of aliases and the names of
# functions (not in the paper).
HASHED = {
    1: {"table": ("name",), "column": ("type",)},
    2: {"table": ("name",), "column": ("type", "name")},
    3: {"table": ("name",), "column": ("type", "name"), "op": ("code",)},
}
ALWAYS = {"scan": ("structure",), "join": ("structure",), "alias": ("role",), "function": ("name",)}


def _md5(*parts) -> str:
    return hashlib.md5("\x1f".join(parts).encode()).hexdigest()


def _date(text: str):
    try:
        d = dt.datetime.fromisoformat(text.strip().replace(" ", "T")[:19])
        return (float(d.year), float(d.month), float(d.day))
    except ValueError:
        return None


NUMBER_TYPES = ("int", "integer", "bigint", "smallint", "decimal", "numeric", "double precision", "double",
                "real", "float", "oid")


def literal_features(g: Graph, i: int) -> list[float]:
    """Return the (literal, value) features, with the extractors of Appendix D."""
    a = g.attr[i]
    text, typ = a["value"], (a["vtype"] or "").split("(")[0].strip()
    if typ.startswith(DATE_TYPES):
        d = _date(text)                                  # f5
        if d is not None:
            return list(d)
    m = None
    if typ in NUMBER_TYPES:
        try:
            m = float(text)
        except ValueError:
            m = None
    if m is not None:
        rng = g.attr[a["col"]]["range"] if a["col"] is not None else None
        if rng is None:
            return [m]                                   # f1
        lo, hi = rng
        f2 = (m - lo) / (hi - lo) if hi > lo else 0.0
        op = a["op"]
        if op in ("<", "<="):                            # f3
            return [0.0, f2]
        if op in (">", ">="):
            return [f2, 1.0]
        if op == "=":
            return [f2, f2]
        return [f2]                                      # f2
    s = text or ""
    return [float(ord(s[k])) if k < len(s) else 0.0 for k in range(3)]   # f4


def node_features(g: Graph, i: int, level: int) -> list[float]:
    """Return the features of one node for F_level (F3 with H3, F2 with H2, F1 with H1)."""
    t = g.type[i]
    if t == "literal":
        return literal_features(g, i)
    if t == "op" and level <= 2:
        return list(ORDINAL.get(g.attr[i]["code"], (0, 0, 0)))           # f8
    if t == "column" and level <= 1:
        x = [g.attr[i]["uniques"]]
        if EXTRA_FEATURES:
            x += list(g.attr[i]["range"] or (0.0, 0.0))                  # f7
        return x
    if t == "table" and EXTRA_FEATURES and level <= 1:
        return [g.attr[i]["size"]]                                       # f6
    return []


def pattern(g: Graph, level: int) -> tuple[str, list[float]]:
    """Return (pattern hash h^H, feature vector x_F^H) of a graph at one level (Eqs. 8-12)."""
    n = len(g.type)
    hashed = HASHED[level]
    h = []
    for i in range(n):                                   # Eq. 8
        t = g.type[i]
        keys = ALWAYS.get(t, ()) + hashed.get(t, ())
        vals = json.dumps([t, len(g.inn[i]), len(g.out[i])] + [g.attr[i].get(k) for k in keys], default=str)
        h.append(_md5(vals))
    order = g.topological()
    for i in order:                                      # Eq. 9: parents, in topological order
        h[i] = _md5(h[i], *sorted(h[k] for k in g.inn[i]))
    for i in reversed(order):                            # Eq. 10: children, in reverse order
        h[i] = _md5(h[i], *sorted(h[k] for k in g.out[i]))
    feats = [node_features(g, i, level) for i in range(n)]
    canon = sorted(range(n), key=lambda i: (h[i], feats[i]))    # pi_H; ties by the features
    key = _md5(*(h[i] for i in canon))                   # Eq. 11
    x = [v for i in canon for v in feats[i]]             # Eq. 12
    return key, x


def describe(req: Request, stats: Stats):
    """Return ({level: pattern hash}, {level: feature vector}) of a question, or None."""
    if req.point not in POINTS:
        return None
    g = Builder(req, stats).g
    keys, feats = {}, {}
    for level in (1, 2, 3):
        keys[level], feats[level] = pattern(g, level)
    return keys, feats


# ---------------------------------------------------------------- the estimator

@register("litecard")
class LiteCard(Estimator):
    """spec: ``litecard`` (the switches are constants of the module)."""

    name = "litecard"

    def __init__(self, arg=None, context=None):
        super().__init__(arg, context)
        if sqlglot is None:
            raise RuntimeError("litecard requires sqlglot (pip install sqlglot)")
        if MODEL == "gbdt" and gbdt_library() is None:
            raise RuntimeError("litecard requires ydf or scikit-learn (pip install scikit-learn)")
        self.buckets: dict[tuple[int, str], tuple[np.ndarray, np.ndarray]] = {}   # (level, hash) -> (X, y)
        self.stats = Stats(None)
        self._answers: dict = {}
        self._rng = random.Random(BIAS_SEED)
        self._native: dict[str, float] = {}

    def fit(self, dataset) -> None:
        self.stats = Stats(StatsCatalog(dataset))
        raw: dict[tuple[int, str], tuple[list, list]] = {}
        for ex in dataset.examples():
            d = describe(ex.request, self.stats)
            if d is None:
                continue
            keys, feats = d
            for level in (1, 2, 3):
                xs, ys = raw.setdefault((level, keys[level]), ([], []))
                xs.append(feats[level])
                ys.append(float(ex.true_rows))
        self.buckets = {}
        for k, (xs, ys) in raw.items():
            width = max(len(x) for x in xs)                     # equal within a pattern (Theorem 3)
            keep = [i for i, x in enumerate(xs) if len(x) == width]
            self.buckets[k] = (np.array([xs[i] for i in keep], dtype=float).reshape(len(keep), width),
                               np.array([ys[i] for i in keep], dtype=float))
        self._answers = {}

    def estimate(self, req: Request):
        own = self._own_estimate(req)                           # recorded for the joins above this one
        d = describe(req, self.stats)
        if d is None:
            return None
        keys, feats = d
        for level in (3, 2, 1):                                 # the most specific level with enough history
            b = self.buckets.get((level, keys[level]))
            if b is None or len(b[1]) < BETA[level] or b[0].shape[1] != len(feats[level]):
                continue
            k = (level, keys[level], tuple(feats[level]), MODEL, TARGET_LOG)
            if k not in self._answers:
                self._answers[k] = self._regress(b[0], b[1], np.array(feats[level], dtype=float))
            floor = 1e-3 if req.is_param else 1.0
            return {"rows": max(self._answers[k], floor), "source": f"litecard-h{level}"}
        return self._bias_correction(req, own)

    # ------------------------------------------------------------ the regressors

    @staticmethod
    def kernel(X: np.ndarray, x: np.ndarray) -> np.ndarray:
        """Return K_sigma between every observation and the question, divided by the largest."""
        if SCALE_FEATURES:
            lo, hi = X.min(axis=0), X.max(axis=0)
            span = np.where(hi > lo, hi - lo, 1.0)
            X, x = (X - lo) / span, (x - lo) / span
        d2 = ((X - x) ** 2).sum(axis=1)
        return np.exp(-(d2 - d2.min()) / SIGMA ** 2)

    def _regress(self, X: np.ndarray, y: np.ndarray, x: np.ndarray) -> float:
        t = np.log1p(y) if TARGET_LOG else y
        if X.shape[1] == 0:                             # no feature: every observation is the same question
            out = float(t.mean())
        else:
            w = self.kernel(X, x)
            if MODEL == "rbf":                          # Eq. 7
                out = float((w * t).sum() / w.sum())
            elif MODEL == "lr":                         # Eq. 5 with a linear g
                Z = (X - X.min(axis=0)) / np.where(np.ptp(X, axis=0) > 0, np.ptp(X, axis=0), 1.0)
                z = (x - X.min(axis=0)) / np.where(np.ptp(X, axis=0) > 0, np.ptp(X, axis=0), 1.0)
                A = np.hstack([Z, np.ones((len(Z), 1))])
                Aw = A * w[:, None]
                theta = np.linalg.solve(A.T @ Aw + RIDGE * np.eye(A.shape[1]), Aw.T @ t)
                out = float(np.append(z, 1.0) @ theta)
            elif MODEL == "gbdt":                       # Eq. 5 with a decision forest
                out = _gbdt(X, t, w, x)
            else:
                raise ValueError(f"unknown MODEL {MODEL!r}")
        return float(np.expm1(out)) if TARGET_LOG else out

    # ------------------------------------------------------------ bias correction

    # The paper corrects PostgreSQL's estimate of the subquery itself. During planning,
    # PostgreSQL estimates a join from its inputs as they stand, and these carry the
    # answers already given for them (learned or corrected). Multiplying that estimate
    # would stack the corrections of every level (up to 1.57 x 20.2 x 1361 x 68656 for
    # five relations). So PostgreSQL's own estimate is recovered first. Every request
    # leaves its own estimate (``self._native``), and for a join
    #     own(S) = pg(S) * own(outer) / rows(outer) * own(inner) / rows(inner),
    # where rows() are the input sizes the planner used (sent with the request).
    # A question without inputs (asked alone, offline) takes pg(S) as its own estimate.

    @staticmethod
    def _scope_key(req: Request, aliases) -> str:
        """Return the relations ``aliases`` of the query of ``req``: tables, aliases and restrictions."""
        aliases = set(aliases)
        rels = sorted((r.rel or "", r.alias) for r in req.rels if r.alias in aliases)
        restr = sorted(c.sql or "<opaque>" for c in req.clauses
                       if len(c.rels) == 1 and c.rels[0] in aliases)
        text = repr((req.queryid, rels, restr))
        return hashlib.md5(text.encode()).hexdigest()

    def _own_estimate(self, req: Request) -> float | None:
        """Return PostgreSQL's estimate of the subquery of ``req`` with uncorrected inputs (None: unknown)."""
        if req.pg is None:
            return None
        split = req.data.get("split")
        if req.point != "join" or split is None:
            own = float(req.pg)
        else:
            outer = set(split.split())
            inner = set(req.aliases) - outer
            o_rows, i_rows = req.data.get("outer_rows"), req.data.get("inner_rows")
            o_own = self._native.get(self._scope_key(req, outer))
            i_own = self._native.get(self._scope_key(req, inner))
            if None in (o_rows, i_rows, o_own, i_own) or not o_rows or not i_rows:
                return None
            own = float(req.pg) * o_own / o_rows * i_own / i_rows
        if req.point == "base" or (req.point == "join" and req.jointype == JOIN_INNER):
            if len(self._native) > NATIVE_MAX:          # a bound on memory, not a cache policy
                self._native.clear()
            self._native[self._scope_key(req, req.aliases)] = own
        return own

    def _bias_correction(self, req: Request, own: float | None):
        if not BIAS_CORRECTION or req.point != "join" or req.jointype != JOIN_INNER or own is None:
            return None
        row = BIAS_TABLE.get(req.n_rels - 1)
        if row is None:
            return None
        proportion, q_error = row
        if BIAS_RANDOM:
            u = self._rng.random()
        else:
            u = int(hashlib.md5(req.key().encode()).hexdigest()[:8], 16) / 0xFFFFFFFF
        if u < proportion:
            return {"rows": max(own * q_error, 1.0), "source": "litecard-bias"}
        return None

    def __getstate__(self):
        state = super().__getstate__()
        state["_answers"] = {}
        state["_native"] = {}
        return state

    def describe(self) -> list[str]:
        n = {level: sum(1 for (lv, _), b in self.buckets.items() if lv == level and len(b[1]) >= BETA[level])
             for level in (1, 2, 3)}
        return [f"{sum(1 for k in self.buckets if k[0] == 3)} patterns at H3; buckets with enough history: "
                f"H3 {n[3]}, H2 {n[2]}, H1 {n[1]}; model {MODEL}"
                f"{' (' + str(gbdt_library()) + ')' if MODEL == 'gbdt' else ''}, target {'log' if TARGET_LOG else 'rows'}, "
                f"bias correction {'on' if BIAS_CORRECTION else 'off'}"]


def gbdt_library() -> str | None:
    """Return the tree library in use: "ydf", "sklearn", or None if neither is installed."""
    import importlib.util
    if GBDT_LIBRARY in ("ydf", "auto") and importlib.util.find_spec("ydf") is not None:
        return "ydf"
    if GBDT_LIBRARY in ("sklearn", "auto") and importlib.util.find_spec("sklearn") is not None:
        return "sklearn"
    return None


def _gbdt(X: np.ndarray, t: np.ndarray, w: np.ndarray, x: np.ndarray) -> float:
    lib = gbdt_library()
    if lib is None:
        raise RuntimeError("litecard with MODEL = 'gbdt' requires ydf or scikit-learn")
    if np.ptp(t) == 0:
        return float(t[0])
    # Both libraries hold out a validation set at random for early stopping. They fail when
    # the weights left for training sum to zero (the kernel weights of far observations
    # underflow, and the near ones may all be held out). So: no early stopping when only a
    # few observations carry weight, and no weight of 0.
    n_eff = float(w.sum()) ** 2 / float((w * w).sum())
    early = n_eff >= 20
    w = np.maximum(w, 1e-12)
    if lib == "sklearn":
        from sklearn.ensemble import GradientBoostingRegressor
        params = dict(GBDT_SKLEARN)
        if not early:
            params.pop("validation_fraction"), params.pop("n_iter_no_change")
        m = GradientBoostingRegressor(**params).fit(X, t, sample_weight=w)
        return float(m.predict(x[None, :])[0])
    import ydf
    data = {f"f{j}": X[:, j] for j in range(X.shape[1])}
    data["y"], data["w"] = t, w
    params = dict(GBDT)
    if not early:
        params["validation_ratio"] = 0.0
    learner = ydf.GradientBoostedTreesLearner(label="y", task=ydf.Task.REGRESSION, weights="w", **params)
    model = learner.train(data, verbose=0)
    return float(np.asarray(model.predict({f"f{j}": x[j:j + 1] for j in range(len(x))})).ravel()[0])
