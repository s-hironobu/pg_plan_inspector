"""GRASP, ported to pgpi.

Wu, Kang, Zhang, Chen, Marcus, Ives: "Data-Agnostic Cardinality Learning from
Imperfect Workloads", PVLDB 18 (2025), arXiv:2506.16007.

GRASP learns from queries and their cardinalities only (no data access). It composes two
kinds of per-table primitives:

* CardEst: the cardinality of a single-table query. The paper uses ArCDF (an
  autoregressive model of the joint CDF with monotone rational-quadratic splines) for
  range predicates, and the set encoding of MSCN for other predicates.
* LCS, a learned count sketch: for each join key of a table, a softmax vector v_q over
  n_lcs buckets. It stands for the distribution of the key in the result of the query.

A join is estimated as  c(q_A) c(q_B) <v_A, v_B>, and in general

    rows = prod_t c(q_t) * prod_{key groups G} || elementwise-prod_{m in G} v_m ||_1

Everything is differentiable. So the primitives are trained end to end, from the
cardinalities of the queries in which their tables appear.

How this implementation differs from the paper (see also README.md in this directory):

* Training data: the nodes of executed plans that pgpi captured (actual rows). The paper
  trains on the final cardinalities of the queries of the workload (``labels=final``).
* It is a re-implementation from the paper, not the code of the authors. The results of
  the paper have not been reproduced with it.
* The formula also covers parameterized questions (rows per outer tuple). The outer
  relations contribute only their key sketches, because their own cardinality cancels.
  So param_base/param_join requests and observations are handled by the same model.
* Mixed predicates: c(q_t) = N_t * P(other preds) * P(range preds | other preds), with
  ArCDF conditioned on the encoding of the other predicates. The paper does not say how
  the two parts are combined.
* Labels: ``labels=final`` uses only the final cardinality of each query, as in the
  paper. ``labels=all`` (default) also uses every intermediate join, base relation and
  parameterized scan that pgpi observed.
* ``censored=1`` also uses nodes that stopped early, as lower bounds (one-sided loss).
* IN lists: the paper does not say how IN lists are encoded. Here the hashed values of
  the literal of a predicate are set for each value of the list (multi-hot).
  With ``in_split=1``, the IN list of a target table is instead split into its values.
  The question is estimated once per value (as ``col = value``), and the estimates are
  added, in training and in estimation. The values of one column are disjoint, so the
  sum is exact for counts and joins alike. A list never seen is answered from its
  values, each learned from every question that uses it. Questions with more than
  ``IN_SPLIT_MAX`` combinations keep the multi-hot encoding. IN lists of outer relations
  of parameterized questions are not split (rows per outer tuple do not add up).
* ``n_lcs`` (sketch width) defaults to 64 with subplan labels and 2048 with final labels
  (the paper uses 100-3000). With labels of every subplan, the sketches must resolve
  small per-key shares, and wide sketches then fit slowly.

Spec: ``grasp`` or ``grasp:labels=final,n_lcs=2048,epochs=300,censored=0,seed=0,restarts=3``.
Requires PyTorch.
"""
from __future__ import annotations

import dataclasses
import hashlib
import itertools
import math
import random
from typing import Any

from ..columns import _array_items
from ..estimator import Estimator, register
from ..predicates import OPS, Composite, Pred, TableQuery, decompose
from ..request import Request

try:
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
except ImportError:  # pragma: no cover
    torch = None
    nn = None

N_HASH = 16
# in_split: at most this many value combinations per question
IN_SPLIT_MAX = 64
MIN_BIN = 1e-3
MIN_DERIV = 1e-3


# ---------------------------------------------------------------- splines

def rq_spline(x, params, K: int):
    """Monotone rational-quadratic spline on [0,1] with f(0)=0, f(1)=1.

    params: (N, 3K-1) unnormalized widths, heights and interior derivatives
    (Durkan et al. 2019, without the inverse).
    """
    W, H, D = params[:, :K], params[:, K:2 * K], params[:, 2 * K:]
    widths = MIN_BIN + (1 - MIN_BIN * K) * F.softmax(W, dim=-1)
    heights = MIN_BIN + (1 - MIN_BIN * K) * F.softmax(H, dim=-1)
    derivs = MIN_DERIV + F.softplus(D)
    one = torch.ones_like(derivs[:, :1])
    derivs = torch.cat([one, derivs, one], dim=-1)                   # (N, K+1)
    cw = F.pad(torch.cumsum(widths, dim=-1), (1, 0))
    ch = F.pad(torch.cumsum(heights, dim=-1), (1, 0))
    cw = torch.cat([cw[:, :-1], torch.ones_like(cw[:, :1])], dim=-1)
    ch = torch.cat([ch[:, :-1], torch.ones_like(ch[:, :1])], dim=-1)
    x = x.clamp(0.0, 1.0)
    idx = (torch.searchsorted(cw[:, 1:].contiguous(), x.unsqueeze(-1).contiguous(),
                              right=False)).clamp(0, K - 1)
    g = lambda t: t.gather(-1, idx).squeeze(-1)                     # noqa: E731
    x0, w, y0, h = g(cw[:, :-1]), g(widths), g(ch[:, :-1]), g(heights)
    d0 = derivs.gather(-1, idx).squeeze(-1)
    d1 = derivs.gather(-1, idx + 1).squeeze(-1)
    delta = h / w
    theta = ((x - x0) / w).clamp(0.0, 1.0)
    tt = theta * (1 - theta)
    num = h * (delta * theta ** 2 + d0 * tt)
    den = delta + (d0 + d1 - 2 * delta) * tt
    return y0 + num / den


class ArCDF(nn.Module if nn else object):
    """Autoregressive joint CDF over d numeric columns, conditioned on a context.

    F(x) = prod_i P(X_i <= x_i | X_<i <= x_<i, ctx). Each conditional is a monotone
    spline of x_i whose parameters depend on x_<i and the context.
    The box selectivity is computed by inclusion-exclusion over the vertices of the box.
    """

    def __init__(self, d: int, ctx_dim: int, K: int = 8, hidden: int = 64):
        super().__init__()
        self.d, self.K = d, K
        self.nets = nn.ModuleList([
            nn.Sequential(nn.Linear(i + ctx_dim, hidden), nn.ReLU(),
                          nn.Linear(hidden, hidden), nn.ReLU(),
                          nn.Linear(hidden, 3 * K - 1))
            for i in range(d)])

    def log_cdf(self, x, ctx):
        out = torch.zeros(x.shape[0], device=x.device)
        for i, net in enumerate(self.nets):
            p = net(torch.cat([x[:, :i], ctx], dim=-1))
            out = out + torch.log(rq_spline(x[:, i], p, self.K).clamp_min(1e-12))
        return out

    def log_box(self, lo, hi, ctx):
        """Return log P(lo < X <= hi | ctx) for each row; lo/hi in [0,1]^d."""
        n, d = lo.shape
        total = torch.zeros(n, device=lo.device)
        for mask in range(1 << d):
            use_lo = torch.tensor([(mask >> i) & 1 for i in range(d)], dtype=torch.bool)
            # vertices that take a lower bound of 0 vanish (F = 0 there)
            active = ~((lo[:, use_lo] <= 0).any(dim=-1)) if use_lo.any() else torch.ones(n, dtype=torch.bool)
            if not active.any():
                continue
            v = torch.where(use_lo.unsqueeze(0), lo, hi)
            sign = -1.0 if bin(mask).count("1") % 2 else 1.0
            f = torch.exp(self.log_cdf(v, ctx))
            total = total + sign * f * active.float()
        return torch.log(total.clamp_min(1e-9))


# ---------------------------------------------------------------- per-table model

class TableModel(nn.Module if nn else object):
    def __init__(self, columns: list[str], keys: list[str], numeric: list[str],
                 n_h: int, n_lcs: int, K: int):
        super().__init__()
        self.columns = {c: i for i, c in enumerate(columns)}
        self.keys = list(keys)
        self.numeric = list(numeric)
        self.feat_dim = len(columns) + 1 + len(OPS) + 2 + N_HASH + 1
        h = n_h
        # CardEst over the non-range predicates (MSCN set encoding)
        self.card_pred = nn.Sequential(nn.Linear(self.feat_dim, h), nn.ReLU(), nn.Linear(h, h), nn.ReLU())
        self.card_out = nn.Sequential(nn.Linear(h, h), nn.ReLU(), nn.Linear(h, 1))
        # LCS: shared encoding of all predicates, one softmax head per join key
        self.lcs_pred = nn.Sequential(nn.Linear(self.feat_dim, h), nn.ReLU(), nn.Linear(h, h), nn.ReLU())
        self.lcs_enc = nn.Sequential(nn.Linear(h, h), nn.ReLU())
        self.lcs_heads = nn.ModuleDict({_k(k): nn.Linear(h, n_lcs) for k in keys})
        # ArCDF over the numeric columns seen in range predicates
        self.arcdf = ArCDF(len(numeric), h, K) if numeric else None
        nn.init.constant_(self.card_out[-1].bias, -2.0)

    @staticmethod
    def _pool(pred_mlp, feats, mask):
        e = pred_mlp(feats) * mask.unsqueeze(-1)
        return e.sum(1) / mask.sum(1, keepdim=True).clamp_min(1.0)

    def forward(self, card_f, card_m, lcs_f, lcs_m, lo, hi):
        """Return log selectivity (B,) and {key: log v (B, n_lcs)}."""
        ce = self._pool(self.card_pred, card_f, card_m)
        log_sel = F.logsigmoid(self.card_out(ce).squeeze(-1))
        # No predicate: the whole table (selectivity 1), not a learned value.
        # Without this, the scale of an unfiltered table and the overlap of its
        # sketches trade off freely when only final cardinalities are known.
        has_preds = card_m.sum(1) > 1.0          # more than the bias token
        log_sel = torch.where(has_preds, log_sel, torch.zeros_like(log_sel))
        if self.arcdf is not None:
            log_sel = log_sel + self.arcdf.log_box(lo, hi, ce)
        le = self.lcs_enc(self._pool(self.lcs_pred, lcs_f, lcs_m))
        logv = {k: F.log_softmax(self.lcs_heads[_k(k)](le), dim=-1) for k in self.keys}
        return log_sel, logv


def _k(col: str) -> str:
    """ModuleDict keys cannot contain dots, so hash the column name."""
    return "k_" + hashlib.sha1(col.encode()).hexdigest()[:12]


# ---------------------------------------------------------------- the estimator

@register("grasp")
class GraspEstimator(Estimator):
    name = "grasp"

    DEFAULTS = {"labels": "all", "n_lcs": 0, "n_h": 64, "epochs": 300, "lr": 3e-3,
                "batch": 64, "censored": 0, "seed": 0, "spline_bins": 8, "restarts": 3, "arcdf": 1,
                "in_split": 0}

    N_LCS_AUTO = {False: 64, True: 2048}

    def __init__(self, arg=None, context=None):
        super().__init__(arg, context)
        if torch is None:
            raise RuntimeError("the grasp estimator requires PyTorch")
        self.cfg = dict(self.DEFAULTS)
        for item in (arg or "").split(","):
            if "=" in item:
                k, v = item.split("=", 1)
                if k not in self.cfg:
                    raise ValueError(f"unknown grasp option {k}")
                self.cfg[k] = type(self.DEFAULTS[k])(v) if not isinstance(self.DEFAULTS[k], str) else v
        if self.cfg["labels"] == "final":
            self.name = "grasp-final"
        if self.cfg["n_lcs"] <= 0:
            # With final-only labels, the overlap alone must bring a PK-FK join down
            # to ~1/N_pk. Its natural (uniform) scale is 1/n_lcs, so a sketch of the
            # size used in the paper is needed. With subplan labels, a small sketch
            # generalizes better.
            self.cfg["n_lcs"] = self.N_LCS_AUTO[self.cfg["labels"] == "final"]
        self.tables: dict[str, dict] = {}      # name -> {size, columns, keys, numeric: {col: (lo, hi)}}
        self.models: dict[str, Any] = {}
        self._cache: dict = {}
        self.history: list[float] = []

    # ------------------------------------------------------------ schema

    def _learn_schema(self, examples, all_examples=()) -> None:
        tables: dict[str, dict] = {}
        nums: dict[tuple, list[float]] = {}
        for ex in examples:
            req = ex.request
            comp, _ = decompose(req)
            if comp is None:
                continue
            for alias, tq in comp.aliases():
                t = tables.setdefault(tq.table, {"size": None, "columns": set(), "keys": set()})
                for p in tq.preds:
                    if p.column:
                        t["columns"].add(p.column)
                    if p.is_range:
                        nums.setdefault((tq.table, p.column), []).append(p.number)
            for g in comp.groups:
                for alias, col in g:
                    tables[dict(comp.aliases())[alias].table]["keys"].add(col)
        # Table sizes (schema information GRASP may use), taken from every
        # single-relation descriptor, whatever the labels in use.
        for ex in list(examples) + list(all_examples):
            req = ex.request
            if req.n_rels == 1 and req.tuples and req.rels[0].rel in tables:
                tables[req.rels[0].rel]["size"] = float(req.tuples)
        for name, t in tables.items():
            t["columns"] = sorted(t["columns"])
            t["keys"] = sorted(t["keys"])
            t["numeric"] = {}
            for (tn, col), vals in nums.items():
                if tn == name:
                    lo, hi = min(vals), max(vals)
                    span = (hi - lo) or max(abs(hi), 1.0)
                    t["numeric"][col] = (lo - 0.1 * span, hi + 0.1 * span)
        self.tables = tables

    # ------------------------------------------------------------ features

    def _pred_feat(self, t: dict, cols: dict, p: Pred):
        f = [0.0] * (len(cols) + 1 + len(OPS) + 2 + N_HASH + 1)
        if p.column in cols:
            f[cols[p.column]] = 1.0
        else:
            f[len(cols)] = 1.0
        o = len(cols) + 1
        f[o + OPS.index(p.op)] = 1.0
        o += len(OPS)
        num = p.number
        if num is not None and p.column in t["numeric"]:
            lo, hi = t["numeric"][p.column]
            f[o] = min(max((num - lo) / (hi - lo), 0.0), 1.0)
            f[o + 1] = 1.0
        o += 2
        if p.value is not None:
            # An IN list sets the bucket of each of its values (multi-hot), so that
            # lists sharing values look alike. Hashing the whole list would make
            # every list an unrelated token.
            for v in (_array_items(p.value) if p.op == "in" else [p.value]):
                h = int(hashlib.md5(v.encode()).hexdigest()[:8], 16) % N_HASH
                f[o + h] = 1.0
        return f

    def _bias_feat(self, cols):
        f = [0.0] * (len(cols) + 1 + len(OPS) + 2 + N_HASH + 1)
        f[-1] = 1.0
        return f

    def _featurize(self, tq: TableQuery):
        """Return, per table query, the card set, the lcs set and the box bounds (cached)."""
        cache = self.__dict__.setdefault("_feat_cache", {})
        r = cache.get(tq)
        if r is None:
            r = cache[tq] = self._featurize_uncached(tq)
        return r

    def _featurize_uncached(self, tq: TableQuery):
        t = self.tables[tq.table]
        model = self.models[tq.table]
        cols = model.columns
        card, lcs = [self._bias_feat(cols)], [self._bias_feat(cols)]
        d = len(model.numeric)
        lo, hi = [0.0] * d, [1.0] * d
        for p in tq.preds:
            fe = self._pred_feat(t, cols, p)
            lcs.append(fe)
            if p.is_range and p.column in model.numeric:
                i = model.numeric.index(p.column)
                lo_c, hi_c = t["numeric"][p.column]
                x = min(max((p.number - lo_c) / (hi_c - lo_c), 0.0), 1.0)
                if p.op in ("<", "<="):
                    hi[i] = min(hi[i], x)
                else:
                    lo[i] = max(lo[i], x)
            else:
                card.append(fe)
        return card, lcs, lo, hi

    # ------------------------------------------------------------ composition

    def _forward(self, insts: list[TableQuery]):
        """Run every table query once. Return log c and log v per instance."""
        by_table: dict[str, list[int]] = {}
        for i, tq in enumerate(insts):
            by_table.setdefault(tq.table, []).append(i)
        log_c = [None] * len(insts)
        log_v = [None] * len(insts)
        for table, idxs in by_table.items():
            model = self.models[table]
            feats = [self._featurize(insts[i]) for i in idxs]

            def pad(sets):
                m = max(len(s) for s in sets)
                fd = len(sets[0][0])
                x = torch.zeros(len(sets), m, fd)
                mask = torch.zeros(len(sets), m)
                for j, s in enumerate(sets):
                    x[j, :len(s)] = torch.tensor(s)
                    mask[j, :len(s)] = 1.0
                return x, mask

            cf, cm = pad([f[0] for f in feats])
            lf, lm = pad([f[1] for f in feats])
            lo = torch.tensor([f[2] for f in feats]).reshape(len(idxs), -1)
            hi = torch.tensor([f[3] for f in feats]).reshape(len(idxs), -1)
            log_sel, logv = model(cf, cm, lf, lm, lo, hi)
            size = math.log(max(self.tables[table]["size"] or 1.0, 1.0))
            for j, i in enumerate(idxs):
                log_c[i] = log_sel[j] + size
                log_v[i] = {k: v[j] for k, v in logv.items()}
        return log_c, log_v

    def _compose(self, comp: Composite, log_c, log_v, base: int, sizes: dict | None = None):
        alias_idx = {a: base + j for j, (a, _) in enumerate(comp.aliases())}
        out = 0.0
        for a, tq in comp.targets:
            lc = log_c[alias_idx[a]]
            if sizes and a in sizes:   # use the fresher table size from the request
                lc = lc - math.log(max(self.tables[tq.table]["size"] or 1.0, 1.0)) + math.log(max(sizes[a], 1.0))
            out = out + lc
        for g in comp.groups:
            s = None
            for a, col in g:
                lv = log_v[alias_idx[a]][col]
                s = lv if s is None else s + lv
            out = out + torch.logsumexp(s, dim=-1)
        return out

    def _variants(self, comp: Composite) -> list[Composite]:
        """With in_split, return the question split over the values of the IN lists of its targets."""
        if not self.cfg["in_split"]:
            return [comp]
        choices, n = [], 1
        for a, tq in comp.targets:
            per_pred = []
            for p in tq.preds:
                if p.op == "in" and p.column and p.value:
                    vals = _array_items(p.value)
                    vt = p.vtype[:-2] if p.vtype and p.vtype.endswith("[]") else p.vtype
                    per_pred.append([dataclasses.replace(p, op="=", value=v, vtype=vt,
                                                         sql=f"({p.column} = {v!r})") for v in vals])
                else:
                    per_pred.append([p])
            opts = [TableQuery(tq.table, tuple(c)) for c in itertools.product(*per_pred)]
            n *= len(opts)
            if n > IN_SPLIT_MAX:
                return [comp]
            choices.append((a, opts))
        if n == 1:
            return [comp]
        return [Composite(targets=[(a, tq) for (a, _), tq in zip(choices, combo)],
                          outers=comp.outers, groups=comp.groups)
                for combo in itertools.product(*(o for _, o in choices))]

    def _supported(self, comp: Composite) -> bool:
        for a, tq in comp.aliases():
            if tq.table not in self.tables or tq.table not in self.models:
                return False
        tab = dict(comp.aliases())
        for g in comp.groups:
            for a, col in g:
                if col not in self.models[tab[a].table].keys:
                    return False
        return True

    # ------------------------------------------------------------ training

    def _training_set(self, dataset):
        exs = list(dataset.examples(usable_only=False)) if self.cfg["censored"] else list(dataset.examples())
        usable = [e for e in exs if not e.censored]
        if self.cfg["labels"] == "final":
            best: dict[int, Any] = {}
            for e in usable:
                r = e.request
                if r.is_param or r.plan_name is not None:
                    continue
                b = best.get(e.seqid)
                if b is None or r.n_rels > b.request.n_rels:
                    best[e.seqid] = e
            chosen = list(best.values())
        else:
            chosen = usable
        lower = []
        if self.cfg["censored"]:
            lower = [e for e in exs if e.censored and not e.request.is_param and e.nloops == 1
                     and e.true_rows and e.true_rows >= 1]
            if self.cfg["labels"] == "final":
                lower = []
        return chosen, lower

    def fit(self, dataset) -> None:
        """Train. With restarts > 1, keep the run with the lowest training loss.

        The sketch heads make the loss non-convex, and some initializations end in
        a clearly worse optimum. The restarts pick the best one on the training data
        only.
        """
        exact, lower = self._training_set(dataset)
        self._learn_schema(exact + lower, dataset.examples(usable_only=False))
        self._feat_cache = {}
        items = []
        for e, kind in [(e, 0) for e in exact] + [(e, 1) for e in lower]:
            comp, _ = decompose(e.request)
            if comp is None or not all(a_tq.table in self.tables for _, a_tq in comp.aliases()):
                continue
            items.append((comp, math.log(max(e.true_rows, 1.0)), kind))
        best = None
        self.restart_losses = []
        for r in range(max(1, self.cfg["restarts"])):
            models, history = self._train_once(items, self.cfg["seed"] * 1000 + r)
            self.restart_losses.append(history[-1] if history else float("inf"))
            if best is None or self.restart_losses[-1] < best[2]:
                best = (models, history, self.restart_losses[-1])
        self.models, self.history = best[0], best[1]
        for m in self.models.values():
            m.eval()
        self._cache = {}

    def _train_once(self, items, seed):
        torch.manual_seed(seed)
        rng = random.Random(seed)
        self.models = {name: TableModel(t["columns"], t["keys"],
                                        sorted(t["numeric"]) if self.cfg["arcdf"] else [],
                                        self.cfg["n_h"], self.cfg["n_lcs"], self.cfg["spline_bins"])
                       for name, t in self.tables.items()}
        items = [it for it in items if self._supported(it[0])]
        self.n_train = len(items)
        if not items:
            return self.models, []
        params = [p for m in self.models.values() for p in m.parameters()]
        opt = torch.optim.Adam(params, lr=self.cfg["lr"])
        epochs = self.cfg["epochs"]
        sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=epochs)
        for m in self.models.values():
            m.train()
        bs = self.cfg["batch"]
        history = []
        items = list(items)
        for ep in range(epochs):
            rng.shuffle(items)
            tot = 0.0
            for s in range(0, len(items), bs):
                batch = items[s:s + bs]
                insts, bases = [], []
                for comp, _, _ in batch:
                    vb = []
                    for v in self._variants(comp):
                        vb.append((v, len(insts)))
                        insts.extend(tq for _, tq in v.aliases())
                    bases.append(vb)
                log_c, log_v = self._forward(insts)
                losses = []
                for (comp, y, kind), vb in zip(batch, bases):
                    parts = [self._compose(v, log_c, log_v, b) for v, b in vb]
                    pred = parts[0] if len(parts) == 1 else torch.logsumexp(torch.stack(parts), 0)
                    err = pred - y
                    losses.append(err ** 2 if kind == 0 else F.relu(-err) ** 2)
                loss = torch.stack(losses).mean()
                opt.zero_grad()
                loss.backward()
                torch.nn.utils.clip_grad_norm_(params, 10.0)
                opt.step()
                tot += loss.item() * len(batch)
            sched.step()
            history.append(tot / len(items))
        return self.models, history

    def __setstate__(self, state):
        super().__setstate__(state)
        self.cfg = {**self.DEFAULTS, **self.cfg}     # for models saved by older versions

    def __getstate__(self):
        state = super().__getstate__()
        state["_cache"] = {}
        state["_feat_cache"] = {}
        return state

    # ------------------------------------------------------------ inference

    def estimate_rows(self, req: Request) -> float | None:
        if not self.models:
            return None
        comp, _ = decompose(req)
        if comp is None or not self._supported(comp):
            return None
        sizes = None
        if req.n_rels == 1 and req.tuples and req.tuples > 0:
            sizes = {comp.targets[0][0]: float(req.tuples)}
        key = (req.key(), tuple(sorted(sizes.items())) if sizes else None)
        if key in self._cache:
            return self._cache[key]
        with torch.no_grad():
            insts, vb = [], []
            for var in self._variants(comp):
                vb.append((var, len(insts)))
                insts.extend(tq for _, tq in var.aliases())
            log_c, log_v = self._forward(insts)
            v = sum(float(torch.exp(self._compose(var, log_c, log_v, b, sizes))) for var, b in vb)
        if len(self._cache) > 100000:
            self._cache.clear()
        self._cache[key] = v
        return v

    def estimate(self, req: Request):
        v = self.estimate_rows(req)
        return {"rows": v} if v is not None else None
