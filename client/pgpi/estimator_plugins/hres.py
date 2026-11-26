"""hres: hierarchical residuals on PostgreSQL's estimate, applied only as far as they are certain.

An example of a method built with this framework. It is a method of this project, not an
implementation of a published one (see README.md in this directory). It is measured only
in the benchmarks.

hres does not learn a row count. It learns the residual of PostgreSQL's estimate,

    r = log(actual / PostgreSQL's estimate)

With no residual learned (r = 0), the answer is PostgreSQL's estimate. Every answer is a
factor on it.

For a join, r is the residual of the join's own selectivity,

    r = log(actual(S) / (actual(L) actual(R))) - log(pg(S) / (pg(L) pg(R)))

where L and R are the two inputs. When the planner asks about a join, its estimate is
already computed from the corrected inputs. Only the selectivity is left to correct.
(``JOIN_LOCAL = False`` learns log(actual(S) / pg(S)) instead. The inputs are then
corrected twice.)

(a) A hierarchy of patterns. Each observation is filed at four levels:

    L4  the same question, literals included
    L3  the same question up to literals
    L2  the same tables with conditions on the same columns
    L1  the same tables

The estimate at a level is the mean of its residuals, shrunk towards the estimate of the
next coarser level (L0: 0, that is PostgreSQL's estimate) by partial pooling,

    m_k = (n_k mean_k + TAU m_{k-1}) / (n_k + TAU)

A pattern seen once moves the answer little. A pattern seen often is trusted. There is no
threshold where the answer jumps.

(b) Uncertainty. Take the finest pattern seen at least twice. The spread s of its
residuals tells how much a new question of that pattern may differ (other literals,
other inputs),

    u = s sqrt(1 + 1/n)

The correction applied is what remains certain,

    c = sign(m) max(|m| - K u, 0)

and the plugin abstains when nothing remains. K is larger for corrections downwards
(K_DOWN) than upwards (K_UP). An underestimate is the costlier error: it leads the planner
to nested loops over inputs far larger than planned.

Related work: patterns at several levels of detail as in LiteCard (Yi et al., 2025,
https://arxiv.org/abs/2510.03386); residuals on the planner's estimate as in TiCard
(https://arxiv.org/abs/2512.14358) and LEO (Stillger et al., VLDB 2001); choosing an
estimate by confidence as in Babcock and Chaudhuri, "Towards a Robust Query Optimizer",
SIGMOD 2005. No novelty is claimed.

The settings are constants of this file on purpose, not options.
"""
from __future__ import annotations

import hashlib
import json
import math

from ..estimator import Estimator, register
from ..metrics import plan_trees
from ..predicates import parse_join, parse_single
from ..request import JOIN_INNER, Request

# ---------------------------------------------------------------- settings

TAU = 2.0            # pooling strength: TAU observations at a level weigh as much as its parent
K_DOWN = 2.0         # a correction downwards must exceed K_DOWN * u
K_UP = 1.0           # the same for a correction upwards
SIGMA0 = 1.0         # spread assumed when no pattern was seen twice (log scale: a factor of e)
LEVELS = (1, 2, 3, 4)  # the levels in use, coarse to fine
JOIN_LOCAL = True    # joins: learn the residual of the join's own selectivity (False: of the row count)

POINTS = ("base", "join", "param_base")


def _md5(what) -> str:
    return hashlib.md5(json.dumps(what, separators=(",", ":")).encode()).hexdigest()


def pattern_keys(req: Request) -> dict[int, str]:
    """The question's pattern at each level."""
    table = {r.alias: r.rel or f"<{r.rtekind}>" for r in req.rels + req.outer}
    head = [req.point, req.jointype, sorted(table[r.alias] for r in req.rels),
            sorted(table[r.alias] for r in req.outer)]
    cols = []
    for c in req.clauses + req.outer_clauses:
        if len(c.rels) == 1:
            p = parse_single(c.sql, c.rels[0])
            cols.append([table.get(c.rels[0], "?"), p.column or "?"])
        else:
            j = parse_join(c.sql)
            cols.append(sorted(f"{table.get(a, '?')}.{col}" for a, col in j) if j else ["?"])
    return {1: _md5(head), 2: _md5(head + [sorted(cols)]), 3: "t" + req.template(), 4: "k" + req.key()}


@register("hres")
class HierarchicalResidual(Estimator):
    """spec: ``hres`` (the settings are constants of the module)."""

    name = "hres"

    def __init__(self, arg=None, context=None):
        super().__init__(arg, context)
        self.stats: dict[tuple[int, str], tuple[int, float, float]] = {}    # (level, key) -> (n, mean, sd)

    # ------------------------------------------------------------ learning

    def fit(self, dataset) -> None:
        exs = [ex for ex in dataset.examples() if ex.request.point in POINTS and ex.est_pg and ex.est_pg > 0]
        obs = []
        for ex in exs:                                          # base relations and parameterized scans first,
            if ex.request.point != "join":                      # because the inputs of joins are corrected with them
                floor = 1.0 if ex.request.point == "base" else 1e-3
                obs.append((ex.request, math.log(max(ex.true_rows, floor) / max(ex.est_pg, floor))))
        self._file(obs)
        obs += self._join_observations(dataset, exs)
        self._file(obs)

    def _file(self, obs) -> None:
        raw: dict[tuple[int, str], list[float]] = {}
        for req, r in obs:
            for level, key in pattern_keys(req).items():
                raw.setdefault((level, key), []).append(r)
        self.stats = {}
        for k, rs in raw.items():
            mean = sum(rs) / len(rs)
            sd = math.sqrt(sum((x - mean) ** 2 for x in rs) / (len(rs) - 1)) if len(rs) > 1 else 0.0
            self.stats[k] = (len(rs), mean, sd)

    def _join_observations(self, dataset, exs) -> list[tuple[Request, float]]:
        by_plan: dict[int, list] = {}
        for ex in exs:
            by_plan.setdefault(ex.seqid, []).append(ex)
        trees = plan_trees(dataset, list(by_plan)) if JOIN_LOCAL else {}
        obs = []
        for seqid, pexs in by_plan.items():
            by_node = {ex.node_id: ex for ex in pexs}
            tree = trees.get(seqid, {"children": {}, "point": {}, "input": {}})

            def below(nid, what):                               # the cardinality node at nid or below it
                while nid is not None:
                    if nid in what:
                        return what[nid]
                    ch = tree["children"].get(nid, [])
                    if len(ch) != 1 or (what is by_node and tree["point"].get(nid)):
                        return None
                    nid = ch[0][1]
                return None

            for ex in pexs:
                req = ex.request
                if req.point != "join" or req.jointype != JOIN_INNER:
                    continue
                r = math.log(max(ex.true_rows, 1.0) / max(ex.est_pg, 1.0))
                if JOIN_LOCAL:
                    ch = tree["children"].get(ex.node_id, [])
                    if len(ch) != 2:
                        continue
                    ok = True
                    for _, nid in ch:
                        e, pg_in = below(nid, by_node), below(nid, tree["input"])
                        actual = self._input_rows(e) if e is not None else None
                        if not actual or pg_in is None or not pg_in[1] or pg_in[1] <= 0:
                            ok = False
                            break
                        r -= math.log(max(actual, 1.0) / max(pg_in[1], 1.0))
                    if not ok:
                        continue
                obs.append((req, r))
        return obs

    def _input_rows(self, ex) -> float | None:
        """The actual rows of a join input, counted as the planner counts them in its estimate of the join."""
        req = ex.request
        if req.point == "join" or req.point == "base":
            return ex.true_rows
        if req.point != "param_base":
            return None
        # The inner side of a nested loop. The relation's own rows are not observed,
        # so use PostgreSQL's estimate of them, corrected as for a base question.
        rr = req.data.get("rel_rows")
        if rr is None:
            return None
        r = req.rels[0]
        own = [c for c in req.data.get("clauses", []) if c.get("rels") == r.alias]
        b = Request({"point": "base", "rels": [req.data["rels"][0]], "outer": [], "clauses": own,
                     "outer_clauses": [], "opaque": req.opaque, "nonrel": req.nonrel, "jointype": 0,
                     "plan_name": req.plan_name, "pg": rr, "tuples": req.tuples})
        c, _ = self.correction(b)
        return rr * math.exp(c)

    # ------------------------------------------------------------ answering

    def correction(self, req: Request) -> tuple[float, int]:
        """Return (the certain part of the log correction, the finest level seen). (0, 0) means abstain."""
        keys = pattern_keys(req)
        m, finest, spread = 0.0, 0, None
        for level in LEVELS:
            s = self.stats.get((level, keys[level]))
            if s is None:
                continue
            n, mean, sd = s
            m = (n * mean + TAU * m) / (n + TAU)
            finest = level
            if n >= 2:
                spread = sd * math.sqrt(1 + 1 / n)
        if finest == 0:
            return 0.0, 0
        u = SIGMA0 if spread is None else spread
        k = K_DOWN if m < 0 else K_UP
        c = math.copysign(max(abs(m) - k * u, 0.0), m)
        return c, finest

    def estimate(self, req: Request):
        if req.point not in POINTS or (req.point == "join" and req.jointype != JOIN_INNER):
            return None
        c, level = self.correction(req)
        if c == 0.0:
            return None
        return {"factor": math.exp(c), "source": f"hres-l{level}"}

    def describe(self) -> list[str]:
        n = {lv: sum(1 for k in self.stats if k[0] == lv) for lv in (1, 2, 3, 4)}
        return [f"patterns: L1 {n[1]}, L2 {n[2]}, L3 {n[3]}, L4 {n[4]}; tau {TAU:g}, k down {K_DOWN:g} up {K_UP:g}, "
                f"joins {'local' if JOIN_LOCAL else 'total'}"]
