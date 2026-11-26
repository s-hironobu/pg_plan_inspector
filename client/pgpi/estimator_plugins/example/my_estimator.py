"""A template for your own estimator.

Use it with any command, by file path and class name:

    python3 -m pgpi qerror --data ds/ --plugin pgpi/estimator_plugins/example/my_estimator.py:MyEstimator
    python3 -m pgpi serve  --socket /tmp/pgpi.sock --data ds/ \
                           --plugin pgpi/estimator_plugins/example/my_estimator.py:MyEstimator

Everything after a second colon is passed to the estimator as ``self.arg``
(``my_estimator.py:MyEstimator:0.5``).
"""

import math
from collections import defaultdict

import pgpi


class MyEstimator(pgpi.Estimator):
    """Example: a correction of the estimate of PostgreSQL for each set of tables.

    The correction is learned in log space. Newer observations count more
    (exponential decay).

    Replace the body of fit() and estimate() with your method.
    """

    name = "mine"

    def __init__(self, arg=None, context=None):
        super().__init__(arg, context)
        self.decay = float(arg) if arg else 0.8
        self.log_ratio = {}

    def fit(self, dataset):
        # dataset.examples() yields pgpi.Example objects:
        #   ex.request     the question (pgpi.Request): point, rels, outer, clauses,
        #                  outer_clauses, jointype, tuples, key(), template(), ...
        #   ex.true_rows   the observed rows (per loop for parameterized points)
        #   ex.est_pg      the estimate of PostgreSQL on the same basis
        # dataset.nodes(), plans() and stats() give the full Arrow tables.
        # dataset.stats_of(dbid, relid, version) gives the statistics of a relation.
        acc = defaultdict(float)
        for ex in sorted(dataset.examples(), key=lambda e: e.seqid):
            if not ex.est_pg:
                continue
            key = (ex.request.point, ex.request.tables)
            r = math.log(max(ex.true_rows, 1) / max(ex.est_pg, 1))
            acc[key] = self.decay * acc[key] + (1 - self.decay) * r if key in acc else r
        self.log_ratio = dict(acc)

    def estimate(self, req: pgpi.Request):
        # Return None to abstain (the value of PostgreSQL is used).
        # Otherwise return pgpi.rows(n), pgpi.factor(f), or pgpi.sel(s) for 'clauselist'.
        r = self.log_ratio.get((req.point, req.tables))
        if r is None or req.point == "clauselist":
            return None
        return pgpi.factor(math.exp(r))
