"""postgres: always abstains, so PostgreSQL's own estimate is used.

It is the baseline that every other estimator is compared with. It is also the
smallest possible plugin: a class with a name and an estimate() that returns None.
"""

from __future__ import annotations

from ..estimator import Estimator, register
from ..request import Request


@register("postgres")
class PostgresEstimator(Estimator):
    """Always abstains: PostgreSQL's own estimate is used. The baseline."""

    name = "postgres"

    def estimate(self, req: Request):
        return None
