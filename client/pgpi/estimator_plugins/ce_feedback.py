"""ce-feedback: per-query correction of wrong model assumptions, like SQL Server 2022's CE feedback.

It starts from PostgreSQL's assumptions and switches them per query (queryid), in three steps:

1. Identify. The executions show a large error on the operators an assumption governs,
   and an alternative exists.
   * Correlation: one step toward full correlation when the actual rows exceed the
     estimate, one step toward independence when they are smaller.
   * Containment: the other model, if it would have been closer on the observed joins.
2. Verify. The next executions run with the alternative. It is kept only if the error on
   those operators went down and the execution time did not regress (``tolerance``).
3. Persist. Kept choices go to a hint store (a JSON file, the counterpart of Query Store
   hints). Rejected choices are not tried again.

How this differs from SQL Server 2022 (see also README.md in this directory):

* Only the feedback on two of SQL Server's model assumptions is reproduced: the
  correlation of filters on one table and the containment of equi-joins. The row-goal
  feedback is not.
* The rest of SQL Server's estimator is not reproduced either: its histograms, ascending
  keys, multi-column statistics, and CE versions beyond these two choices. The statistics
  are PostgreSQL's, as captured in the dataset.
* Queries are told apart by PostgreSQL's queryid instead of a query hash.
* The analysis runs when ``fit`` is called, not in the background.
* The hint store is a JSON file instead of the Query Store.

Each call of ``fit`` (or ``python3 -m pgpi cefeedback``) is one round of analysis over
the observations collected since the last round. Queries run with hints are left alone,
as in SQL Server.

How the answers are computed (AssumptionsEstimator below). The model assumptions
themselves (correlation levels, containment models, hints) are in pgpi/assume.py.
Scans (base, param_base) get the correlation factor. Inner equi-joins get the
containment factor. For a live join request, only the clauses between the two inputs
that the planner used are counted (``split``), because the inputs already carry the
factors of their own clauses. The statistics (MCVs, histograms, n_distinct) come from
the dataset given to ``fit`` or ``load_stats``. ``pgpi.hints`` overrides the assumptions
per session or transaction, like ``OPTION (USE HINT (...))``:

    SET LOCAL pgpi.hints = 'ASSUME_MIN_SELECTIVITY_FOR_FILTER_ESTIMATES';

With ``load_stats`` only (no analysis) and no store, ce-feedback answers every query with
the fixed assumptions of its options. This is the counterpart of a database-wide setting
(SQL Server's compatibility level): ``ce-feedback:correlation=partial``.

References: "Cardinality estimation (CE) feedback",
https://learn.microsoft.com/en-us/sql/relational-databases/performance/intelligent-query-processing-cardinality-estimation-feedback;
"Cardinality Estimation (SQL Server)",
https://learn.microsoft.com/en-us/sql/relational-databases/performance/cardinality-estimation-sql-server;
J. Sack, "Optimizing Your Query Plans with the SQL Server 2014 Cardinality Estimator",
Microsoft white paper, 2014, https://learn.microsoft.com/en-us/previous-versions/dn673537(v=msdn.10).
"""
from __future__ import annotations

import json
import math
import os
import statistics

from typing import Any

from .. import assume as A
from ..columns import SUPPORTED_OPS, StatsCatalog
from ..estimator import Estimator, register
from ..predicates import parse_join, parse_single
from ..request import JOIN_INNER, Request


class AssumptionsEstimator(Estimator):
    """Answer with fixed model assumptions (pgpi/assume.py). It is the base of ce-feedback and is not registered."""

    name = "assumptions"
    DEFAULTS: dict = {"correlation": "independence", "containment": "base", "hints": 1}

    def __init__(self, arg=None, context=None):
        super().__init__(arg, context)
        self.cfg = dict(self.DEFAULTS)
        for item in (arg or "").split(","):
            if "=" in item:
                k, v = item.split("=", 1)
                if k == "name":
                    self.name = v
                    continue
                if k not in self.cfg:
                    raise ValueError(f"unknown {self.name} option {k}")
                d = self.DEFAULTS[k]
                self.cfg[k] = v if isinstance(d, str) else type(d)(v)
        self.default = A.Model(self.cfg["correlation"], self.cfg["containment"])
        self.catalog: StatsCatalog | None = None
        self._bins: dict = {}

    # ------------------------------------------------------------ statistics

    def fit(self, dataset) -> None:
        self.load_stats(dataset)

    def load_stats(self, dataset) -> None:
        """Take the statistics (MCVs, histograms, n_distinct) from a dataset."""
        self.catalog = StatsCatalog(dataset)
        self._bins = {}

    def _col(self, table: str, col: str):
        if (table, col) not in self._bins:
            self._bins[(table, col)] = self.catalog.bins(table, col) if self.catalog else None
        return self._bins[(table, col)]

    def _reltuples(self, table: str) -> float | None:
        st = self.catalog.by_rel.get(table) if self.catalog else None
        return float(st.get("reltuples") or 0) or None if st else None

    def _filters(self, req: Request, alias: str, table: str) -> dict[str, float]:
        """Return the selectivity of the filters on each column of one relation (supported columns only)."""
        by_col: dict[str, list | None] = {}
        for c in req.clauses:
            if tuple(c.rels) != (alias,):
                continue
            p = parse_single(c.sql, alias)
            if p.column is None or p.op not in SUPPORTED_OPS:
                if p.column is not None:
                    by_col[p.column] = None
                continue
            if by_col.get(p.column, []) is not None:
                by_col.setdefault(p.column, []).append(p)
        out = {}
        for col, preds in by_col.items():
            if not preds:
                continue
            b = self._col(table, col)
            cov = b.coverage(preds) if b else None
            if cov is not None:
                out[col] = sum(x * f for x, f in zip(cov, b.freqs))
        return out

    # ------------------------------------------------------------ factors

    def correlation_factor(self, req: Request, model: A.Model) -> float | None:
        """Return combine(s) / prod(s) for the target relation of a scan."""
        if req.point not in ("base", "param_base") or req.n_rels != 1 or not req.rels[0].rel:
            return None
        r = req.rels[0]
        sels = list(self._filters(req, r.alias, r.rel).values())
        if len(sels) < 2:
            return None
        prod = math.prod(sels)
        if prod <= 0:
            return None
        return A.combine(sels, model.correlation) / prod

    def containment_factor(self, req: Request, model: A.Model, level_only: bool = True) -> float | None:
        """Return the product over the equi-join clauses of simple/base (1 for base containment)."""
        if req.point != "join" or req.jointype != JOIN_INNER:
            return None
        tables = {r.alias: r.rel for r in req.rels if r.rel}
        split = set((req.data.get("split") or "").split()) if level_only else set()
        f, used = 1.0, False
        filt: dict[str, dict] = {}
        for c in req.clauses:
            if len(c.rels) != 2:
                continue
            j = parse_join(c.sql)
            if j is None or j[0][0] not in tables or j[1][0] not in tables:
                continue
            (a, x), (b, y) = j
            if split and ((a in split) == (b in split)):
                continue                            # inside one input: already counted in its rows
            sides = []
            for alias, col in ((a, x), (b, y)):
                t = tables[alias]
                if alias not in filt:
                    filt[alias] = self._filters(req, alias, t)
                fs = filt[alias]
                bins, n = self._col(t, col), self._reltuples(t)
                if bins is None or not n:
                    break
                nd = bins.n_distinct_rest + len(bins.mcv)
                others = [s for k, s in fs.items() if k != col]
                sides.append(A.JoinSide(nd, n, fs.get(col, 1.0), A.combine(others, model.correlation)))
            if len(sides) != 2:
                continue
            used = True
            if model.containment == "simple":
                f *= A.containment_factor(*sides)
        return f if used else None

    # ------------------------------------------------------------ answers

    def model_for(self, req: Request) -> tuple[A.Model, str]:
        """Return the model for this request and where it came from (hint / default)."""
        if self.cfg["hints"]:
            m, hinted = A.parse_hints(req.data.get("hints"), self.default)
            if hinted:
                return m, "hint"
        return self.default, "default"

    def estimate(self, req: Request) -> Any:
        model, origin = self.model_for(req)
        return self._answer(req, model, origin)

    def _answer(self, req, model, origin):
        if model == A.POSTGRES:
            return None
        if req.point in ("base", "param_base"):
            f = self.correlation_factor(req, model)
        else:
            f = self.containment_factor(req, model, level_only="split" in req.data)
        if f is None or abs(f - 1.0) < 1e-12:
            return None
        return {"factor": f, "source": f"{origin}:{model.name}"}


@register("ce-feedback")
class CEFeedbackEstimator(AssumptionsEstimator):
    name = "ce-feedback"
    DEFAULTS = {"correlation": "independence", "containment": "base", "hints": 1,
                "store": "", "threshold": 2.0, "tolerance": 0.2, "slack_ms": 5.0}

    def __init__(self, arg=None, context=None):
        super().__init__(arg, context)
        self.store: dict[str, dict] = {}
        if self.cfg["store"] and os.path.exists(self.cfg["store"]):
            with open(self.cfg["store"]) as f:
                self.store = json.load(f)
        self.decisions: list[str] = []

    # ------------------------------------------------------------ serving

    def model_for(self, req: Request) -> tuple[A.Model, str]:
        m, origin = super().model_for(req)
        if origin == "hint":
            return m, origin                       # hinted queries: feedback is not used
        e = self.store.get(str(req.queryid)) if req.queryid else None
        if e:
            if e.get("candidate"):
                return A.Model.parse(e["candidate"]), "testing"
            return A.Model.parse(e["model"]), "feedback"
        return self.default, "default"

    # ------------------------------------------------------------ analysis

    def fit(self, dataset) -> None:
        """Load the statistics and run one round of analysis (use load_stats for statistics only)."""
        self.load_stats(dataset)
        self.analyze(dataset)

    def _affected(self, ex) -> str | None:
        """Return the assumption that governs this node: 'correlation', 'containment' or None."""
        req = ex.request
        if req.point in ("base", "param_base") and req.n_rels == 1 and req.rels[0].rel:
            r = req.rels[0]
            return "correlation" if len(self._filters(req, r.alias, r.rel)) >= 2 else None
        if req.point == "join" and req.jointype == JOIN_INNER:
            tables = {r.alias: r.rel for r in req.rels if r.rel}
            for c in req.clauses:
                j = parse_join(c.sql) if len(c.rels) == 2 else None
                if j and j[0][0] in tables and j[1][0] in tables:
                    fa = self._filters(req, j[0][0], tables[j[0][0]])
                    fb = self._filters(req, j[1][0], tables[j[1][0]])
                    if fa and fb:
                        return "containment"
        return None

    @staticmethod
    def _logq(est, true):
        return math.log(max(true, 1.0) / max(est or 0.0, 1.0))

    def analyze(self, dataset) -> list[str]:
        """One round: verify the running candidates, identify new ones, save the store."""
        th = math.log(self.cfg["threshold"])
        plans = {p["seqid"]: p for p in dataset.plans(["seqid", "queryid", "execution_ms"]).to_pylist()}
        hinted = set()
        try:
            for n in dataset.nodes(["seqid", "est_source"]).to_pylist():
                if n["est_source"] and str(n["est_source"]).startswith("hint:"):
                    hinted.add(n["seqid"])
        except Exception:
            pass
        by_q: dict[str, list] = {}
        for ex in dataset.examples():
            if ex.queryid:
                by_q.setdefault(str(ex.queryid), []).append(ex)
        out = []
        for qid, exs in sorted(by_q.items()):
            e = self.store.setdefault(qid, {"model": self.default.name, "candidate": None,
                                            "rejected": [], "since": 0, "history": []})
            exs = [x for x in exs if x.seqid > e["since"] and x.seqid not in hinted]
            if not exs:
                continue
            last = max(x.seqid for x in exs)
            ms = statistics.median([plans[s]["execution_ms"] or 0.0 for s in {x.seqid for x in exs}
                                    if s in plans])
            cur = A.Model.parse(e["candidate"] or e["model"])
            groups: dict[str, list] = {"correlation": [], "containment": []}
            for x in exs:
                k = self._affected(x)
                if k:
                    groups[k].append(x)
            if e.get("candidate"):
                kind = e["kind"]
                errs = [abs(self._logq(x.est_plan, x.true_rows)) for x in groups[kind]]
                err = statistics.median(errs) if errs else e["baseline"]["err"]
                b = e["baseline"]
                ok = err < b["err"] - 1e-9 and ms <= b["ms"] * (1 + self.cfg["tolerance"]) + self.cfg["slack_ms"]
                verdict = "kept" if ok else "rejected"
                msg = (f"{qid}: {e['candidate']} {verdict} (error {math.exp(b['err']):.2f} -> {math.exp(err):.2f}, "
                       f"time {b['ms']:.1f} -> {ms:.1f} ms)")
                if ok:
                    e["model"] = e["candidate"]
                else:
                    e["rejected"].append(e["candidate"])
                e["history"].append(msg)
                e["candidate"] = None
                e["since"] = last
                out.append(msg)
                continue
            cand, kind, err = self._identify(cur, groups, th, e["rejected"])
            if cand:
                e["candidate"], e["kind"] = cand.name, kind
                e["baseline"] = {"err": err, "ms": ms}
                msg = f"{qid}: try {cand.name} ({kind} error {math.exp(err):.2f})"
                e["history"].append(msg)
                out.append(msg)
            e["since"] = last
        self.decisions = out
        self._save()
        return out

    def _identify(self, cur: A.Model, groups, th, rejected):
        """Return one step from the current model, or (None, None, 0)."""
        g = groups["correlation"]
        if g:
            signed = statistics.median([self._logq(x.est_plan, x.true_rows) for x in g])
            err = statistics.median([abs(self._logq(x.est_plan, x.true_rows)) for x in g])
            if abs(signed) >= th:
                i = A.CORRELATION.index(cur.correlation) + (1 if signed > 0 else -1)
                if 0 <= i < len(A.CORRELATION):
                    m = cur.with_(correlation=A.CORRELATION[i])
                    if m.name not in rejected:
                        return m, "correlation", err
        g = groups["containment"]
        if g:
            errs = [abs(self._logq(x.est_plan, x.true_rows)) for x in g]
            err = statistics.median(errs)
            if err >= th:
                alt = cur.with_(containment="simple" if cur.containment == "base" else "base")
                if alt.name not in rejected:
                    new = []
                    for x in g:
                        fc = self.containment_factor(x.request, cur, level_only=False) or 1.0
                        fa = self.containment_factor(x.request, alt, level_only=False) or 1.0
                        new.append(abs(self._logq((x.est_plan or 0) * fa / fc, x.true_rows)))
                    if statistics.median(new) < err:          # a viable alternative
                        return alt, "containment", err
        return None, None, 0.0

    def _save(self):
        if self.cfg["store"]:
            tmp = self.cfg["store"] + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self.store, f, indent=1, sort_keys=True)
            os.replace(tmp, self.cfg["store"])

    def describe(self) -> list[str]:
        out = []
        for qid, e in sorted(self.store.items()):
            if e["model"] != self.default.name or e.get("candidate") or e["rejected"]:
                out.append(f"{qid}: {e['model']}"
                           + (f" (testing {e['candidate']})" if e.get("candidate") else "")
                           + (f", rejected {', '.join(e['rejected'])}" if e["rejected"] else ""))
        return out
