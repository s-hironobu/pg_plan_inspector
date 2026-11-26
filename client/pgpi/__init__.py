"""pg_plan_inspector 2: a framework for adaptive query optimization research on PostgreSQL.

    import pgpi
    ds = pgpi.Dataset("ds/")                     # observations (Parquet / Arrow)
    est = pgpi.load("feedback")                  # or your own pgpi.Estimator subclass
    est.fit(ds)
    pgpi.EstimatorServer([est], "/tmp/pgpi.sock").serve_forever()
"""

__version__ = "0.11.0"

from .request import Request, Rel, Clause, rows, factor, sel, answer_rows  # noqa: E402,F401
from .estimator import Estimator, register, load, registered  # noqa: E402,F401


def __getattr__(name):
    # heavier modules (pyarrow) are imported on first use
    if name == "Dataset":
        from .dataset import Dataset
        return Dataset
    if name == "Example":
        from .dataset import Example
        return Example
    if name == "EstimatorServer":
        from .server import EstimatorServer
        return EstimatorServer
    if name == "Anonymizer":
        from .anonymize import Anonymizer
        return Anonymizer
    raise AttributeError(name)
