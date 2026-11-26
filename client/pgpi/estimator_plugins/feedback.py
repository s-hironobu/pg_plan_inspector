"""A minimal adaptive estimator: learn corrections from executed plans.

The smallest useful AQO loop, written against the plugin API:

1. Run a workload with ``pgpi.log_plans = on``.
2. ``python3 -m pgpi collect`` the observations.
3. ``python3 -m pgpi serve --plugin feedback --data ds/`` and set ``pgpi.mode = enforce``.
4. The same queries, or queries of the same classes, are now planned with
   corrected cardinalities.

Two memories:

* exact: the same question (descriptor with literals) -> the observed rows.
* class: the same question up to literals (template) -> a correction factor
  applied to PostgreSQL's new estimate.

How the class factor is learned matters. PostgreSQL estimates a join from its
inputs: rows(S) = sigma(S) * product of the base relations' rows. Once the base
relations are corrected, PostgreSQL's join estimate already carries those
corrections. So the factor for a join must correct only sigma(S), the *local*
join selectivity:

    kappa(S) = (true(S) / pg(S)) / product_b (true(b) / pg(b))

``feedback:naive`` learns true(S)/pg(S) instead and applies it on top of
already corrected inputs. It shows how much this double correction hurts the
plans. The method is deliberately simple.

The idea of correcting estimates with execution feedback goes back to
M. Stillger, G. Lohman, V. Markl, M. Kandil, "LEO - DB2's LEarning Optimizer",
VLDB 2001 (https://research.ibm.com/publications/leo-db2s-learning-optimizer).

Origin: a method of this project, not an implementation of a published one
(see README.md in this directory). Its performance has not been compared with
published results.
"""

from __future__ import annotations

import math
import statistics

from ..estimator import Estimator, register
from ..request import JOIN_INNER, Request


def _clamp(x: float) -> float:
    return max(float(x), 1.0)


@register("feedback")
class FeedbackEstimator(Estimator):
    """spec: ``feedback`` | ``feedback:exact`` | ``feedback:class`` | ``feedback:naive``."""

    name = "feedback"

    def __init__(self, arg=None, context=None):
        super().__init__(arg, context)
        mode = arg or "both"
        self.use_exact = mode in ("both", "exact", "naive")
        self.use_class = mode in ("both", "class", "naive")
        self.local = mode != "naive"
        if mode == "naive":
            self.name = "feedback-naive"
        self.exact: dict[str, float] = {}
        self.factor: dict[str, float] = {}
        self.n_class: dict[str, int] = {}

    def fit(self, dataset) -> None:
        examples = list(dataset.examples())
        exact: dict[str, list[float]] = {}
        logs: dict[str, list[float]] = {}

        # per plan: true/pg of each base relation, by (subquery, rti)
        base_ratio: dict[int, dict[tuple, float]] = {}
        for ex in examples:
            req = ex.request
            if req.n_rels != 1 or ex.est_pg is None:
                continue
            r = req.rels[0]
            k = (req.plan_name, r.rti)
            plan = base_ratio.setdefault(ex.seqid, {})
            if req.point == "base":
                plan[k] = _clamp(ex.true_rows) / _clamp(ex.est_pg)
            elif req.point == "param_base":
                # A parameterized scan says nothing about the size of the
                # relation itself, unless the relation has no filter of its own.
                own = [c for c in req.clauses if c.rels == (r.alias,)]
                if not own:
                    plan.setdefault(k, 1.0)

        for ex in examples:
            req = ex.request
            exact.setdefault(req.key(), []).append(ex.true_rows)
            if ex.est_pg is None or ex.est_pg <= 0:
                continue
            ratio = _clamp(ex.true_rows) / _clamp(ex.est_pg)
            if self.local and req.point == "join":
                if req.jointype != JOIN_INNER:
                    continue
                plan = base_ratio.get(ex.seqid, {})
                inputs = 1.0
                for r in req.rels:
                    b = plan.get((req.plan_name, r.rti))
                    if b is None:
                        inputs = None
                        break
                    inputs *= b
                if inputs is None:
                    continue
                ratio /= inputs
            logs.setdefault(req.template(), []).append(math.log(ratio))

        self.exact = {k: statistics.median(v) for k, v in exact.items()}
        self.factor = {k: math.exp(sum(v) / len(v)) for k, v in logs.items()}
        self.n_class = {k: len(v) for k, v in logs.items()}

    def estimate(self, req: Request):
        if self.use_exact:
            v = self.exact.get(req.key())
            if v is not None:
                return {"rows": v, "source": f"{self.name}-exact"}
        if self.use_class:
            f = self.factor.get(req.template())
            if f is not None:
                return {"factor": f, "source": f"{self.name}-class"}
        return None
