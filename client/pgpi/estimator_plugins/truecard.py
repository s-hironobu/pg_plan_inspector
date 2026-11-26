"""True cardinalities looked up from observations."""

from __future__ import annotations

import statistics

from ..estimator import Estimator, register
from ..request import Request, rows


@register("truecard")
class TrueCardEstimator(Estimator):
    """Answer with the true value observed for exactly the same question.

    It is fitted from a dataset (uncensored executed nodes). It abstains for questions
    never observed. It is useful as a free partial oracle, and to replay a workload with
    its true cardinalities without running COUNT(*) queries.

    spec: ``truecard`` (fit with --data) or ``truecard:path/to/dataset``.
    """

    name = "truecard"

    def __init__(self, arg=None, context=None):
        super().__init__(arg, context)
        self.values: dict[str, float] = {}
        if arg:
            from ..dataset import Dataset
            self.fit(Dataset(arg))

    def fit(self, dataset) -> None:
        obs: dict[str, list[float]] = {}
        for ex in dataset.examples():
            obs.setdefault(ex.request.key(), []).append(ex.true_rows)
        for k, v in obs.items():
            self.values[k] = statistics.median(v)

    def add(self, req: Request, value: float) -> None:
        self.values[req.key()] = float(value)

    def estimate(self, req: Request):
        v = self.values.get(req.key())
        return rows(v) if v is not None else None

    def __len__(self) -> int:
        return len(self.values)
