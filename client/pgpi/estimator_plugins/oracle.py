"""The oracle: true cardinalities by COUNT(*).

The "perfect estimator" of cardinality-estimation research. It is the upper bound
when comparing estimators. It also shows the plans the planner chooses when every
cardinality is exact.
"""

from __future__ import annotations

from ..estimator import Estimator, register
from ..request import JOIN_INNER, Request, rows

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None


@register("oracle")
class OracleEstimator(Estimator):
    """Run COUNT(*) on its own connection (with pgpi.mode = off).

    It answers base and inner-join questions exactly. It answers parameterized ones
    (any number of outer relations) as rows per combination of outer tuples, which is
    what PostgreSQL estimates there. It abstains when a clause could not be deparsed,
    when a relation is not a table, and for outer/semi/anti joins.

    context: ``dsn`` (required), ``statement_timeout_ms`` (default 600000).
    Results are cached by SQL text. ``cache`` can be shared between instances.
    """

    name = "oracle"

    def __init__(self, arg=None, context=None):
        super().__init__(arg, context)
        if psycopg is None:
            raise RuntimeError("the oracle estimator requires psycopg")
        dsn = self.context.get("dsn")
        if dsn is None:
            raise ValueError("the oracle estimator needs a dsn")
        self.conn = psycopg.connect(dsn, autocommit=True)
        self.conn.execute("SET pgpi.mode TO off")
        self.conn.execute("SET pgpi.log_plans TO off")
        self.conn.execute("SET max_parallel_workers_per_gather TO 0")
        tmo = int(self.context.get("statement_timeout_ms", 600000))
        self.conn.execute(f"SET statement_timeout TO {tmo}")
        self.cache: dict[str, int] = self.context.get("oracle_cache", {})
        self.queries = 0

    def _count(self, sql: str) -> int:
        if sql not in self.cache:
            self.cache[sql] = self.conn.execute(sql).fetchone()[0]
            self.queries += 1
        return self.cache[sql]

    def true_rows(self, req: Request) -> float | None:
        """Return the true value on the basis of the request (per loop for param points)."""
        if not req.complete:
            return None
        if req.point in ("join", "param_join") and req.jointype != JOIN_INNER:
            return None
        if not req.outer:
            sql = req.count_sql()
            return float(self._count(sql)) if sql else None
        sqls = req.param_sqls()
        if sqls is None:
            return None
        num_sql, den_sqls = sqls
        den = 1.0
        for d in den_sqls:
            den *= self._count(d)
        if den == 0:
            return None
        return self._count(num_sql) / den

    def estimate(self, req: Request):
        v = self.true_rows(req)
        return rows(v) if v is not None else None

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
