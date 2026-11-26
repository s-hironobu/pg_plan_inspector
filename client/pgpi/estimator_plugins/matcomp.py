"""matcomp: join cardinality as matrix completion over the join keys.

Idea. Inside one table, the statistics are (or can be made) accurate: PostgreSQL's
per-column and extended statistics cover every column set of a table. No per-table
statistic sees how two tables meet. They meet only through the join keys. For two
inputs L and R of a join on key k

    |L join R| = sum_k cnt_L(k) * cnt_R(k)
               = |L| * |R| * sum_k p_L(k) * p_R(k)

where p_L is the distribution of the key in L *after L's own predicates*. Every non-key
column has already been summed out inside its table. The only unknown is the coupling

    y(L, R) = log( |L join R| / (|L| * |R|) ) = log <p_L, p_R>.

Write p = 1/K + t, where t is how L's predicates tilt the key distribution away from
uniform over K keys. Then <p_L, p_R> = (1/K) (1 + K <t_L, t_R>): the uniform part times
one plus an inner product of two tilt vectors in key space. Key space is huge, but tilts
are few and correlated (a region, a category or a date range shifts the keys in a handful
of ways). So a low-rank compression of the tilts should do.

This turns the problem into the Netflix problem. Rows and columns are what was filtered
on each side (region r3, category c7, ...). Entries are the observed couplings. The
actual rows of executed joins fill some entries, and the model completes the rest:

    y(L, R) = mu_e + a_L + a_R + log(1 + u_L' u_R)

* mu_e: the coupling of the join edge e (the join clauses between L and R, literals
  removed). It is the uniform part, about log(1/K).
* u_S: the compressed tilt of side S, the sum of the k-dimensional embeddings of its
  predicates (``side=atoms``, the default: one per single-relation clause, so
  "region = 'r3'" is the same row of the matrix however the planner splits the join).
  a_S is a bias, built the same way.
* The bias of a predicate is the bias of its *template* (the clause without the literal)
  plus a regularized deviation of its own. Its embedding is its own only. So a new
  literal keeps the average coupling of its template and adds no interaction
  (PostgreSQL's independence, 1 + 0). A template seen in other queries carries its bias
  into a new join template. (A shared template embedding would make u_L' u_R a constant
  that the biases cancel. The fit would stay near the floor of log(1 + z), where it is
  unstable.)

There is no metric matrix between the embeddings. The two sides are different
predicates, so u_L' u_R takes either sign. A free matrix would only trade scale with the
embeddings and defeat the regularization.

``form=log`` uses u_L' u_R instead of log(1 + u_L' u_R): a skew that is low-rank in key
space is not low-rank in log space.
``side=whole`` makes each side (its relations and all its predicates) one row of the
matrix, instead of a sum of predicates.

The prediction for a live join request is exp(y) * outer_rows * inner_rows, with the
inputs as the planner has them. The extension sends ``split`` / ``outer_rows`` /
``inner_rows``. The offline evaluation takes them from the plan tree. Base questions are
answered by an internal ``feedback`` estimator (``base=none`` to abstain). Inner joins only.

Spec: ``matcomp`` or
``matcomp:rank=2,lam=0.3,steps=3000,lr=0.05,restarts=5,form=linear,side=atoms,base=feedback,name=...``.
The fit is not convex. It keeps the best of ``restarts`` random starts (the lowest
regularized loss).
NumPy only.

Origin: a method of this project, not an implementation of a published one
(see README.md in this directory). Its performance has not been compared with
published results.
"""
from __future__ import annotations

import json
import math
from typing import Any

import numpy as np

from ..estimator import Estimator, register
from ..metrics import plan_trees
from ..request import JOIN_INNER, Request, strip_literals
from .feedback import FeedbackEstimator


def split_sides(req: Request, outer: str):
    """Split a join request into (left aliases, right aliases, left clauses, right clauses, edge clauses).

    Clauses are (sql, aliases) pairs."""
    left = set(outer.split())
    right = {r.alias for r in req.rels} - left
    if not left or not right:
        return None
    lc, rc, ec = [], [], []
    for c in req.clauses:
        s = set(c.rels)
        item = (c.sql if c.sql is not None else "<opaque>", s)
        if s and s <= left:
            lc.append(item)
        elif s and s <= right:
            rc.append(item)
        else:
            ec.append(item)
    return left, right, lc, rc, ec


def edge_key(edge: list) -> str:
    return json.dumps(sorted(strip_literals(sql) for sql, _ in edge), separators=(",", ":"))


@register("matcomp")
class MatComp(Estimator):
    name = "matcomp"
    DEFAULTS = {"rank": 2, "lam": 0.3, "steps": 3000, "lr": 0.05, "restarts": 5, "form": "linear",
                "side": "atoms", "base": "feedback", "seed": 0}
    SOFT = 0.05
    FLOOR = 0.01

    def __init__(self, arg=None, context=None):
        super().__init__(arg, context)
        self.cfg = dict(self.DEFAULTS)
        for item in (arg or "").split(","):
            if "=" not in item:
                continue
            k, v = item.split("=", 1)
            if k == "name":
                self.name = v
                continue
            if k not in self.cfg:
                raise ValueError(f"unknown matcomp option {k}")
            self.cfg[k] = type(self.DEFAULTS[k])(v)
        for k, ok in (("form", ("linear", "log")), ("side", ("atoms", "whole")), ("base", ("feedback", "none"))):
            if self.cfg[k] not in ok:
                raise ValueError(f"matcomp {k} must be one of {', '.join(ok)}")
        self.base = FeedbackEstimator() if self.cfg["base"] == "feedback" else None
        self.edges: dict[str, int] = {}
        self.tmpls: dict[str, int] = {}
        self.atoms: dict[str, int] = {}
        self.params: dict[str, np.ndarray] = {}
        self.n_obs = 0
        self.loss = None

    # ------------------------------------------------------------ sides as rows of the matrix

    def side_atoms(self, req: Request, aliases: set, clauses: list) -> list[tuple[str, str]]:
        """Return (key with literals, template) for each item of one side. Each item is a row of the matrix."""
        if self.cfg["side"] == "whole":
            rels = sorted([r.rel or f"<{r.rtekind}>", r.alias] for r in req.rels if r.alias in aliases)
            sqls = sorted(sql for sql, _ in clauses)
            return [(json.dumps([rels, sqls], separators=(",", ":")),
                     json.dumps([rels, sorted(strip_literals(s) for s in sqls)], separators=(",", ":")))]
        return sorted((sql, strip_literals(sql)) for sql, rels in clauses if len(rels) == 1)

    # ------------------------------------------------------------ training data

    def _input_rows(self, ex) -> float | None:
        """The rows of an input, counted as the planner's join request counts them."""
        req = ex.request
        if req.point != "param_base":
            return ex.true_rows
        # A parameterized inner scan: use the relation's own (unparameterized) rows,
        # corrected the way the base estimator would correct them.
        rr = req.data.get("rel_rows")
        if rr is None or self.base is None:
            return rr
        r = req.rels[0]
        own = [c for c in req.data.get("clauses", []) if c.get("rels") == r.alias]
        b = Request({"point": "base", "rels": [req.data["rels"][0]], "outer": [], "clauses": own,
                     "outer_clauses": [], "opaque": req.opaque, "nonrel": req.nonrel, "jointype": 0,
                     "plan_name": req.plan_name, "pg": rr, "tuples": req.tuples})
        ans = self.base.estimate(b)
        if ans and "rows" in ans:
            return ans["rows"]
        if ans and "factor" in ans:
            return rr * ans["factor"]
        return rr

    def observations(self, dataset) -> list[tuple]:
        """Return (edge, left atoms, right atoms, y) for each executed inner join."""
        exs = list(dataset.examples())
        by_plan: dict[int, list] = {}
        for ex in exs:
            by_plan.setdefault(ex.seqid, []).append(ex)
        trees = plan_trees(dataset, list(by_plan))
        obs = []
        for seqid, pexs in by_plan.items():
            by_node = {ex.node_id: ex for ex in pexs}
            tree = trees.get(seqid, {"children": {}, "point": {}})

            def card(nid):
                while nid is not None:
                    if nid in by_node:
                        return by_node[nid]
                    ch = tree["children"].get(nid, [])
                    if len(ch) != 1 or tree["point"].get(nid):
                        return None
                    nid = ch[0][1]
                return None

            for ex in pexs:
                req = ex.request
                if req.point != "join" or req.jointype != JOIN_INNER:
                    continue
                ch = tree["children"].get(ex.node_id, [])
                o = [n for role, n in ch if role == "outer"]
                i = [n for role, n in ch if role == "inner"]
                eo, ei = (card(o[0]) if o else None), (card(i[0]) if i else None)
                if eo is None or ei is None:
                    continue
                xo, xi = self._input_rows(eo), self._input_rows(ei)
                if not xo or not xi:
                    continue
                parts = split_sides(req, " ".join(r.alias for r in eo.request.rels))
                if parts is None:
                    continue
                left, right, lc, rc, ec = parts
                y = math.log(max(ex.true_rows, 1.0) / (max(xo, 1.0) * max(xi, 1.0)))
                obs.append((edge_key(ec), self.side_atoms(req, left, lc), self.side_atoms(req, right, rc), y))
        return obs

    # ------------------------------------------------------------ the model

    def _link(self, z):
        """The interaction term of y and its derivative.

        linear: log(FLOOR + softplus(1 + z)). It is about log(1 + z), positive, and
        never below log(FLOOR), so a confident negative tilt cannot drive an
        estimate to zero. log: z.
        """
        if self.cfg["form"] == "log":
            return z, np.ones_like(z)
        s = self.SOFT
        x = (1.0 + z) / s
        sp = self.FLOOR + s * np.logaddexp(0.0, x)
        sig = 1.0 / (1.0 + np.exp(-np.clip(x, -50, 50)))
        return np.log(sp), sig / sp

    def _indicators(self, sides: list[list[tuple[str, str]]]):
        A = np.zeros((len(sides), len(self.atoms)))
        T = np.zeros((len(sides), len(self.tmpls)))
        for n, atoms in enumerate(sides):
            for key, tmpl in atoms:
                if key in self.atoms:
                    A[n, self.atoms[key]] += 1
                if tmpl in self.tmpls:
                    T[n, self.tmpls[tmpl]] += 1
        return A, T

    def fit(self, dataset) -> None:
        if self.base is not None:
            self.base.fit(dataset)
        obs = self.observations(dataset)
        self.n_obs = len(obs)
        self.edges, self.tmpls, self.atoms = {}, {}, {}
        if not obs:
            self.params = {}
            return
        for e, la, ra, _ in obs:
            self.edges.setdefault(e, len(self.edges))
            for key, tmpl in la + ra:
                self.atoms.setdefault(key, len(self.atoms))
                self.tmpls.setdefault(tmpl, len(self.tmpls))
        E = np.array([self.edges[o[0]] for o in obs])
        AL, TL = self._indicators([o[1] for o in obs])
        AR, TR = self._indicators([o[2] for o in obs])
        y = np.array([o[3] for o in obs])
        best = None
        for restart in range(max(1, self.cfg["restarts"])):
            P, mse, obj = self._train(E, AL, TL, AR, TR, y, self.cfg["seed"] + 1000 * restart)
            if best is None or obj < best[2]:
                best = (P, mse, obj)
        self.params, self.loss = best[0], best[1]

    def _train(self, E, AL, TL, AR, TR, y, seed):
        """One run of Adam from a random start. Return (parameters, training MSE, objective)."""
        n, k = len(y), self.cfg["rank"]
        lam = self.cfg["lam"]
        rng = np.random.default_rng(seed)
        mu0 = np.zeros(len(self.edges))
        np.add.at(mu0, E, y)
        mu0 /= np.bincount(E, minlength=len(self.edges))
        P = {"mu": mu0, "alpha": np.zeros(len(self.tmpls)), "delta": np.zeros(len(self.atoms)),
             "Eps": rng.normal(0, 0.1, (len(self.atoms), k))}
        m = {kk: np.zeros_like(v) for kk, v in P.items()}
        s = {kk: np.zeros_like(v) for kk, v in P.items()}
        b1, b2, lr = 0.9, 0.999, self.cfg["lr"]
        for step in range(1, self.cfg["steps"] + 1):
            uL = AL @ P["Eps"]
            uR = AR @ P["Eps"]
            z = np.sum(uL * uR, axis=1)
            fz, dfz = self._link(z)
            pred = (P["mu"][E] + TL @ P["alpha"] + AL @ P["delta"] + TR @ P["alpha"] + AR @ P["delta"] + fz)
            r = pred - y
            g = 2 * r / n
            gz = g * dfz
            duL = gz[:, None] * uR
            duR = gz[:, None] * uL
            G = {"mu": np.bincount(E, weights=g, minlength=len(self.edges)),
                 "alpha": TL.T @ g + TR.T @ g,
                 "delta": AL.T @ g + AR.T @ g,
                 "Eps": AL.T @ duL + AR.T @ duR}
            # ridge: strong on a predicate's own terms, light on its template's bias
            G["delta"] += 2 * lam * P["delta"] / n
            G["Eps"] += 2 * lam * P["Eps"] / n
            G["alpha"] += 2 * 0.1 * lam * P["alpha"] / n
            for kk in P:
                m[kk] = b1 * m[kk] + (1 - b1) * G[kk]
                s[kk] = b2 * s[kk] + (1 - b2) * G[kk] ** 2
                P[kk] -= lr * (m[kk] / (1 - b1 ** step)) / (np.sqrt(s[kk] / (1 - b2 ** step)) + 1e-8)
        reg = (lam * (np.sum(P["delta"] ** 2) + np.sum(P["Eps"] ** 2))
               + 0.1 * lam * np.sum(P["alpha"] ** 2)) / n
        mse = float(np.mean(r ** 2))
        return P, mse, mse + float(reg)

    def coupling(self, req: Request, outer: str) -> float | None:
        """Return log(|L join R| / (|L| |R|)) for the split of req into outer and the rest."""
        if not self.params or req.jointype != JOIN_INNER:
            return None
        parts = split_sides(req, outer)
        if parts is None:
            return None
        left, right, lc, rc, ec = parts
        e = self.edges.get(edge_key(ec))
        if e is None:
            return None
        P = self.params
        (AL, TL), (AR, TR) = (self._indicators([self.side_atoms(req, left, lc)]),
                              self._indicators([self.side_atoms(req, right, rc)]))
        uL = (AL @ P["Eps"])[0]
        uR = (AR @ P["Eps"])[0]
        a = float((TL @ P["alpha"] + AL @ P["delta"] + TR @ P["alpha"] + AR @ P["delta"])[0])
        fz, _ = self._link(np.array([uL @ uR]))
        return float(P["mu"][e] + a + fz[0])

    def estimate(self, req: Request) -> Any:
        if req.point != "join":
            return self.base.estimate(req) if self.base is not None else None
        xo, xi = req.data.get("outer_rows"), req.data.get("inner_rows")
        outer = req.data.get("split") or req.data.get("outer_aliases")
        if xo is None or xi is None or not outer:
            return None
        y = self.coupling(req, outer)
        if y is None:
            return None
        v = math.exp(y) * max(xo, 1.0) * max(xi, 1.0)
        if not math.isfinite(v):
            return None
        return {"rows": max(v, 1.0)}

    def describe(self) -> list[str]:
        if not self.params:
            return ["no join observations"]
        what = "predicates" if self.cfg["side"] == "atoms" else "sides"
        return [f"{self.n_obs} join observations: {len(self.edges)} join edges, {len(self.atoms)} {what} "
                f"({len(self.tmpls)} templates), rank {self.cfg['rank']}, form {self.cfg['form']}, "
                f"training MSE (log) {self.loss:.3f}"]
