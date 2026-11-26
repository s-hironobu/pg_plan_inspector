"""NNGP: cardinality by a Gaussian process with the kernel of an infinitely wide network.

An implementation, as a pgpi plugin, of

    K. Zhao, J. X. Yu, Z. He, R. Li, H. Zhang, "Lightweight and Accurate Cardinality
    Estimation by Neural Network Gaussian Process", SIGMOD 2022
    (arXiv 2107.08706: "Uncertainty-aware Cardinality Estimation by Neural Network
    Gaussian Process").

It is written from the paper. No code of the authors was found. The paper builds the
estimator on Neural Tangents (JAX). Here the kernel is computed in closed form with
NumPy, which gives the same kernel. Where the paper leaves a choice open, the choice
made here is marked "not in the paper" below.

The method, as implemented here
-------------------------------

1. Query encoding (Section 2). A question is one fixed-length vector. It is the
   concatenation, in a fixed order, of

   * for every attribute of every relation (selection part):
     - a numeric attribute: the interval [lb, ub] of its range condition, min-max
       normalized to [0, 1] by the domain of the attribute;
     - a categorical attribute: the bitmap of the values its IN or = condition allows.
       For a large domain, the factorized bitmap: the bitmap is cut into slices of s
       bits, and each slice is read as an integer;
   * for every joinable pair of attributes (join part): 3 bits for <, =, >
     (000: no join condition on the pair).

   Self-joins are told apart by renaming the relations. Here the alias is used, for the
   tables that occur more than once in a question of the workload.

2. The model (Section 4). A Gaussian process whose kernel is the NNGP kernel of a fully
   connected ReLU network of infinite width (Eqs. 17-19). It is computed exactly with
   the arc-cosine formula for ReLU:

       K^0(x, x')  = sigma_b^2 + sigma_w^2 <x, x'> / d
       K^l(x, x')  = sigma_b^2 + sigma_w^2 / (2 pi) sqrt(K^{l-1}(x, x) K^{l-1}(x', x'))
                     (sin t + (pi - t) cos t),   t = arccos(normalized K^{l-1}(x, x'))

   Training is exact Bayesian inference. The posterior mean (Eq. 9) and variance
   (Eq. 10) of log(cardinality) (Eq. 21) are computed in closed form, with no gradient
   descent.

3. Uncertainty (Section 5). The posterior variance gives the coefficient of variation
   (standard deviation over mean) of each estimate. The paper uses it to tell how
   reliable an estimate is, and to pick queries for active learning.

How this implementation differs from the paper (see also README.md in this directory)
------------------------------------------------------------------------------------

* Not given in the paper: the number of hidden layers, sigma_w^2, sigma_b^2 and the
  noise on the training labels (Eq. 7). The values below are the choice of this file:
  DEPTH = 1, as in the derivation of the paper; sigma_w^2 = 2, the variance that keeps
  ReLU activations at the scale of their inputs; a small sigma_b^2; a small noise, as a
  diagonal regularization relative to the mean diagonal of the kernel.
* Not given in the paper: how an attribute without a condition is encoded (here: the
  whole domain, [0, 1] or every bit set), the domain of a categorical attribute (here:
  the values seen in the conditions of the workload, plus one bit for every other
  value), the slice width s of the factorized bitmap, and which attributes are
  categorical (here: those of non-numeric types, and numeric ones that the workload only
  ever compares with = or IN, such as ids). The domain of a numeric attribute is taken
  from PostgreSQL's statistics as captured in the dataset (pg_statistic).
* Not in the paper: one bit per relation, telling which relations the question covers.
  The paper encodes whole queries. pgpi asks about every subquery. Without these bits, a
  scan of one table without conditions and a scan of another would have the same vector.
* Not in the paper: the labels are centred (their mean is subtracted) before the
  zero-mean process is fitted, and the mean is added back to the predictions.
* Questions this encoding cannot express are not answered (PostgreSQL's estimate stands):
  conditions other than ranges, =, <> and IN (LIKE, OR, functions); join conditions
  between attributes that no question of the workload joined; attributes or relations
  not seen in the workload; outer joins and parameterized questions.
* The training set is capped at MAX_TRAIN observations, because exact GP inference is
  cubic in their number. Questions with the same vector are merged (mean of their
  labels). The paper trains on 60% of 2,000 to 4,000 generated queries.
* Not in the paper: a question about a set of relations that no observation covered is
  not answered (``SEEN_RELATIONS``). pgpi learns only from the subqueries the planner
  chose to execute. The kernel puts an unseen set of relations (e.g. role_type,
  cast_info and name, which PostgreSQL never joins first on CEB) near seen questions
  that share most of its bits (role_type alone). The process then answers with
  confidence (a small coefficient of variation) and far off, and the planner picks
  exactly those subqueries. Found on CEB 1a.
* Not given in the paper: the size from which a factorized bitmap is used
  (``BITMAP_MAX``, large here: the slices, read as integers up to 2^SLICE - 1, would
  outweigh the 0/1 positions in the kernel).
* By default, every other question in the reach of the encoding is answered, as in the
  paper. MAX_CV (not in the paper) makes the plugin abstain when the coefficient of
  variation of an estimate exceeds it.
* Training data, as for all pgpi plugins: the nodes of executed plans (every subquery
  pgpi observed), not generated queries labeled with true cardinalities.

The settings are constants of this file on purpose, not options. NumPy only.
"""
from __future__ import annotations

import math
import random

import numpy as np

from ..columns import StatsCatalog, _array_items
from ..estimator import Estimator, register
from ..predicates import NUMERIC_TYPES, parse_join, parse_single, to_number
from ..request import JOIN_INNER, Request

# ---------------------------------------------------------------- settings

DEPTH = 1               # hidden layers (not given in the paper; its derivation uses one)
SIGMA_W2 = 2.0          # weight variance (not given in the paper)
SIGMA_B2 = 0.05         # bias variance (not given in the paper)
NOISE = 1e-3            # label noise, relative to the mean diagonal of the kernel (not given)
BITMAP_MAX = 10000      # categorical domains up to this size: a plain bitmap (not given in the paper;
                        # large, since the integers of a factorized bitmap would dominate the kernel)
SLICE = 8               # larger ones: the factorized bitmap, slices of SLICE bits (not given)
MAX_TRAIN = 4000        # exact inference is cubic in the number of observations
MAX_CV = None           # abstain above this coefficient of variation (None: as in the paper)
SEEN_RELATIONS = True   # abstain on a set of relations no observation covered (not in the paper)
SEED = 0

POINTS = ("base", "join")
JOIN_CODE = {"<": (1, 0, 0), "=": (0, 1, 0), ">": (0, 0, 1), "<=": (1, 1, 0), ">=": (0, 1, 1), "<>": (1, 0, 1)}


# ---------------------------------------------------------------- the kernel

def relu_expectation(kxy: np.ndarray, kxx: np.ndarray, kyy: np.ndarray) -> np.ndarray:
    """Return E[relu(u) relu(v)] for (u, v) Gaussian with covariances kxx, kxy, kyy (arc-cosine)."""
    norm = np.sqrt(np.maximum(kxx * kyy, 1e-300))
    c = np.clip(kxy / norm, -1.0, 1.0)
    t = np.arccos(c)
    return norm / (2 * math.pi) * (np.sin(t) + (math.pi - t) * c)


def nngp_kernel(X: np.ndarray, Y: np.ndarray, diag_y: bool = False):
    """Return K(X, Y) of the network (Eqs. 17-19). With diag_y, also K(y, y) for every row of Y."""
    d = max(X.shape[1], 1)
    kxy = SIGMA_B2 + SIGMA_W2 * (X @ Y.T) / d
    kxx = SIGMA_B2 + SIGMA_W2 * (X * X).sum(axis=1) / d
    kyy = SIGMA_B2 + SIGMA_W2 * (Y * Y).sum(axis=1) / d
    for _ in range(DEPTH):
        kxy = SIGMA_B2 + SIGMA_W2 * relu_expectation(kxy, kxx[:, None], kyy[None, :])
        kxx = SIGMA_B2 + SIGMA_W2 * relu_expectation(kxx, kxx, kxx)
        kyy = SIGMA_B2 + SIGMA_W2 * relu_expectation(kyy, kyy, kyy)
    return (kxy, kyy) if diag_y else kxy


# ---------------------------------------------------------------- the encoding

class Encoder:
    """The fixed layout of the query vector, learned from the questions of the workload."""

    def __init__(self, catalog: StatsCatalog | None):
        self.catalog = catalog
        self.rels: list[str] = []                   # relation keys
        self.attrs: list[tuple[str, str]] = []      # (relation key, column)
        self.kind: dict = {}                        # attr -> "num" | "cat"
        self.domain: dict = {}                      # num: (lo, hi); cat: [values]
        self.pairs: list[tuple] = []                # joinable attribute pairs
        self.renamed: set[str] = set()              # tables that occur twice in a question
        self.literal_type: dict = {}                # attr -> type of its literals

    # ------------------------------------------------------------ parsing a question

    def rel_key(self, table: str, alias: str) -> str:
        return f"{table}:{alias}" if table in self.renamed else table

    def parse(self, req: Request):
        """Return ({rel key}, {attr: [preds]}, {pair: op}) of a question, or None if it cannot be encoded."""
        if req.point not in POINTS or req.outer or (req.point == "join" and req.jointype != JOIN_INNER):
            return None
        table = {r.alias: r.rel for r in req.rels}
        if any(t is None for t in table.values()):
            return None
        keys = {a: self.rel_key(t, a) for a, t in table.items()}
        preds: dict = {}
        joins: dict = {}
        for c in req.clauses:
            if len(c.rels) == 1:
                a = c.rels[0]
                p = parse_single(c.sql, a)
                if p.column is None or p.op not in ("=", "<>", "<", "<=", ">", ">=", "in"):
                    return None
                preds.setdefault((keys[a], p.column), []).append((p, table[a]))
            elif len(c.rels) == 2:
                j = parse_join(c.sql)
                if j is None:
                    return None
                (a, x), (b, y) = j
                if a not in keys or b not in keys:
                    return None
                l, r = (keys[a], x), (keys[b], y)
                joins[tuple(sorted((l, r)))] = "="
            else:
                return None
        return set(keys.values()), preds, joins

    # ------------------------------------------------------------ the layout

    def fit(self, reqs: list[Request]) -> None:
        for r in reqs:
            tables = [x.rel for x in r.rels if x.rel]
            self.renamed |= {t for t in tables if tables.count(t) > 1}
        rels, ops, values, pairs = set(), {}, {}, set()
        for r in reqs:
            parsed = self.parse(r)
            if parsed is None:
                continue
            rs, preds, joins = parsed
            rels |= rs
            pairs |= set(joins)
            for attr, ps in preds.items():
                for p, table in ps:
                    ops.setdefault(attr, set()).add(p.op)
                    if p.vtype:
                        self.literal_type.setdefault(attr, p.vtype.split("(")[0].replace("[]", "").strip())
                    vals = _array_items(p.value) if p.op == "in" else [p.value]
                    values.setdefault(attr, set()).update(v for v in vals or [] if v is not None)
        self.rels = sorted(rels)
        self.attrs = sorted(ops)
        self.pairs = sorted(pairs)
        for attr in self.attrs:
            typ = self.column_type(attr)
            numeric = typ in NUMERIC_TYPES
            if numeric and not ops[attr] <= {"=", "in", "<>"}:
                lo, hi = self.numeric_domain(attr, typ, values.get(attr, ()))
                self.kind[attr], self.domain[attr] = "num", (lo, hi)
            else:
                self.kind[attr], self.domain[attr] = "cat", sorted(values.get(attr, ()))

    def table_of(self, key: str) -> str:
        return key.split(":", 1)[0]

    def column_stats(self, attr):
        st = self.catalog.by_rel.get(self.table_of(attr[0])) if self.catalog else None
        return next((c for c in (st or {}).get("columns", []) if c.get("attname") == attr[1] and not c.get("inh")), None)

    def column_type(self, attr) -> str:
        c = self.column_stats(attr)
        if c is not None:
            return (c.get("type") or "").split("(")[0].strip()
        return self.literal_type.get(attr, "-")          # no statistics: use the type of its literals

    def numeric_domain(self, attr, typ, seen) -> tuple[float, float]:
        """Return [min, max] of a numeric attribute: from the statistics, widened by the literals seen."""
        vals = [v for v in (to_number(x, typ) for x in seen) if v is not None]
        c = self.column_stats(attr)
        for s in (c or {}).get("slots", []):
            if s.get("kind") in (1, 2):
                vals += [v for v in (to_number(x, typ) for x in s.get("values", [])) if v is not None]
        if not vals:
            return (0.0, 1.0)
        return (min(vals), max(vals))

    def width_of(self, attr) -> int:
        if self.kind[attr] == "num":
            return 2
        m = len(self.domain[attr]) + 1                  # + every other value
        return m if m <= BITMAP_MAX else math.ceil(m / SLICE)

    @property
    def width(self) -> int:
        return len(self.rels) + sum(self.width_of(a) for a in self.attrs) + 3 * len(self.pairs)

    # ------------------------------------------------------------ encoding

    def encode(self, req: Request) -> np.ndarray | None:
        parsed = self.parse(req)
        if parsed is None:
            return None
        rels, preds, joins = parsed
        if not rels <= set(self.rels) or not set(preds) <= set(self.kind) or not set(joins) <= set(self.pairs):
            return None
        x = [1.0 if r in rels else 0.0 for r in self.rels]
        for attr in self.attrs:
            ps = preds.get(attr, [])
            x += self._interval(attr, ps) if self.kind[attr] == "num" else self._bitmap(attr, ps)
        for pair in self.pairs:
            x += list(JOIN_CODE[joins[pair]]) if pair in joins else [0, 0, 0]
        return np.array(x, dtype=float)

    def _interval(self, attr, ps) -> list[float]:
        lo, hi = self.domain[attr]
        span = hi - lo if hi > lo else 1.0
        a, b = 0.0, 1.0
        for p, table in ps:
            v = to_number(p.value, p.vtype or self.column_type(attr))
            if v is None:
                continue
            z = min(max((v - lo) / span, 0.0), 1.0)
            if p.op in ("<", "<="):
                b = min(b, z)
            elif p.op in (">", ">="):
                a = max(a, z)
            elif p.op == "=":
                a, b = max(a, z), min(b, z)
        return [a, b]

    def _bitmap(self, attr, ps) -> list[float]:
        dom = self.domain[attr]
        index = {v: i for i, v in enumerate(dom)}
        bits = [1] * (len(dom) + 1)                     # no condition: every value is allowed
        for p, table in ps:
            vals = _array_items(p.value) if p.op == "in" else [p.value]
            allowed = [0] * (len(dom) + 1)
            for v in vals or []:
                allowed[index.get(v, len(dom))] = 1
            if p.op == "<>":
                allowed = [1 - b for b in allowed]
                allowed[len(dom)] = 1                   # "other" still holds the other values
            bits = [b & c for b, c in zip(bits, allowed)]
        if len(bits) <= BITMAP_MAX:
            return [float(b) for b in bits]
        out = []                                        # the factorized bitmap
        for i in range(0, len(bits), SLICE):
            out.append(float(int("".join(str(b) for b in bits[i:i + SLICE]), 2)))
        return out


# ---------------------------------------------------------------- the estimator

@register("nngp")
class NNGP(Estimator):
    """spec: ``nngp`` (the settings are constants of the module)."""

    name = "nngp"

    def __init__(self, arg=None, context=None):
        super().__init__(arg, context)
        self.encoder: Encoder | None = None
        self.X = np.zeros((0, 0))
        self.alpha = np.zeros(0)
        self.chol = None
        self.chol_inv = None
        self.mean = 0.0
        self.seen_rels: set[bytes] = set()

    def fit(self, dataset) -> None:
        exs = [ex for ex in dataset.examples() if ex.request.point in POINTS]
        self.encoder = Encoder(StatsCatalog(dataset))
        self.encoder.fit([ex.request for ex in exs])
        merged: dict[bytes, list] = {}
        nrel = len(self.encoder.rels)
        self.seen_rels = set()
        for ex in exs:
            x = self.encoder.encode(ex.request)
            if x is None:
                continue
            self.seen_rels.add(x[:nrel].tobytes())
            merged.setdefault(x.tobytes(), [x, []])[1].append(math.log(max(float(ex.true_rows), 1.0)))
        rows = list(merged.values())
        if len(rows) > MAX_TRAIN:
            rows = random.Random(SEED).sample(rows, MAX_TRAIN)
        if not rows:
            self.X, self.alpha, self.chol = np.zeros((0, self.encoder.width)), np.zeros(0), None
            return
        self.X = np.stack([x for x, _ in rows])
        y = np.array([sum(ys) / len(ys) for _, ys in rows])
        self.mean = float(y.mean())
        K = nngp_kernel(self.X, self.X)
        K[np.diag_indices_from(K)] += NOISE * float(np.mean(np.diag(K)))
        self.chol = np.linalg.cholesky(K)
        # Invert the Cholesky factor once, so that a question costs O(n^2), not O(n^3).
        self.chol_inv = np.linalg.inv(self.chol)
        self.alpha = self.chol_inv.T @ (self.chol_inv @ (y - self.mean))

    def predict(self, req: Request) -> tuple[float, float] | None:
        """Return (posterior mean, posterior standard deviation) of log(rows), or None."""
        if self.encoder is None or self.chol is None:
            return None
        x = self.encoder.encode(req)
        if x is None:
            return None
        if SEEN_RELATIONS and x[:len(self.encoder.rels)].tobytes() not in self.seen_rels:
            return None
        kx, kxx = nngp_kernel(self.X, x[None, :], diag_y=True)
        kx = kx[:, 0]
        mu = self.mean + float(kx @ self.alpha)                      # Eq. 9
        v = self.chol_inv @ kx
        var = max(float(kxx[0] - v @ v), 0.0)                        # Eq. 10
        return mu, math.sqrt(var)

    def estimate(self, req: Request):
        p = self.predict(req)
        if p is None:
            return None
        mu, sd = p
        cv = sd / abs(mu) if mu else float("inf")
        if MAX_CV is not None and cv > MAX_CV:
            return None
        return {"rows": max(math.exp(mu), 1.0), "source": f"nngp (cv {cv:.2f})"}

    def describe(self) -> list[str]:
        if self.encoder is None:
            return ["not fitted"]
        e = self.encoder
        return [f"{len(self.X)} observations; vector of {e.width}: {len(e.rels)} relations, "
                f"{sum(1 for a in e.attrs if e.kind[a] == 'num')} numeric and "
                f"{sum(1 for a in e.attrs if e.kind[a] == 'cat')} categorical attributes, {len(e.pairs)} join pairs; "
                f"depth {DEPTH}, sigma_w^2 {SIGMA_W2:g}, sigma_b^2 {SIGMA_B2:g}"]
