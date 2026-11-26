"""The bundled estimators, one plugin per file. Importing this package registers them.

The file of a plugin is its name with '-' replaced by '_' (``ce-feedback`` ->
ce_feedback.py). Each file is a complete plugin, written against the same API as
your own (pgpi.Estimator: fit() and estimate()). The simple ones are good starting
points:

    postgres.py       always abstains (PostgreSQL's own estimate); the baseline
    oracle.py         true row counts by COUNT(*); the yardstick for evaluation
    truecard.py       observed true row counts, for the same requests only
    feedback.py       observed values for observed requests; the simplest AQO
    grasp.py          per-table components assembled for unseen join shapes (PyTorch)
    ce_feedback.py    per-query switching of model assumptions (SQL Server's CE feedback)
    matcomp.py        join affinity by matrix completion over join keys
    litecard.py       a small local regressor per recurring subquery pattern (LiteCard; ydf, sqlglot)
    nngp.py           a Gaussian process with the kernel of an infinitely wide network (NNGP)
    hres.py           hierarchical residuals on PostgreSQL's estimate, applied as far as they are certain

Code shared by several plugins is outside this directory, in pgpi/:
predicates.py (structured predicates), columns.py (bins from statistics),
assume.py (the model assumptions of ce-feedback). matcomp builds on feedback.

PyTorch is imported only when an estimator that needs it is instantiated.
"""

from . import postgres, oracle, truecard  # noqa: F401  (baselines and validation)
from . import feedback, grasp  # noqa: F401
from . import ce_feedback  # noqa: F401
from . import matcomp  # noqa: F401
from . import litecard, hres, nngp  # noqa: F401
