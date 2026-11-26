"""The plugin API.

A researcher writes one class::

    from pgpi import Estimator, rows

    class MyEstimator(Estimator):
        name = "mine"

        def fit(self, dataset):              # optional: learn from observations
            ...

        def estimate(self, req):             # answer a cardinality question
            if req.point != "join":
                return None                  # abstain: PostgreSQL's own value is used
            return rows(42)

and uses it everywhere by its spec ``path/to/file.py:MyEstimator`` or
``package.module:MyEstimator``:

    python3 -m pgpi serve  --socket /tmp/pgpi.sock --plugin mymod.py:MyEstimator --data ds/
    python3 -m pgpi qerror --data ds/ --plugin mymod.py:MyEstimator

``estimate`` returns None (abstain), a number (rows), or a dict with one of
``rows`` / ``factor`` / ``sel`` (see pgpi.request). For parameterized points,
rows are per outer tuple. ``req.is_param`` tells which case it is.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import pickle
import sys
from typing import Any, Callable

from .request import Request


class Estimator:
    """Base class of all estimators."""

    #: short name used in logs and reports
    name: str = "estimator"

    def __init__(self, arg: str | None = None, context: dict | None = None):
        self.arg = arg
        self.context = context or {}

    def fit(self, dataset) -> None:  # pragma: no cover - default does nothing
        """Learn from an observation dataset (pgpi.Dataset). Optional."""

    def estimate(self, req: Request) -> Any:
        """Answer the question, or return None to abstain."""
        return None

    def close(self) -> None:
        """Release resources (connections...)."""

    # Persistence: pickle by default. Override it for large models.
    def save(self, path: str) -> None:
        with open(path, "wb") as f:
            pickle.dump(self, f)

    @staticmethod
    def load_saved(path: str) -> "Estimator":
        with open(path, "rb") as f:
            return pickle.load(f)

    def __getstate__(self):
        state = dict(self.__dict__)
        state.pop("context", None)      # may hold connections
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.context = {}


_REGISTRY: dict[str, Callable[[str | None, dict], Estimator]] = {}


def register(name: str):
    """Class decorator that makes an estimator available by a short name."""
    def deco(cls):
        _REGISTRY[name] = cls
        return cls
    return deco


def registered() -> list[str]:
    _load_builtins()
    return sorted(_REGISTRY)


def _load_builtins() -> None:
    from . import estimator_plugins  # noqa: F401  (registers the bundled plugins)


def _import_from(path_or_module: str):
    if path_or_module.endswith(".py") or os.sep in path_or_module:
        path = os.path.abspath(path_or_module)
        modname = "pgpi_plugin_" + os.path.splitext(os.path.basename(path))[0]
        spec = importlib.util.spec_from_file_location(modname, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load {path}")
        mod = importlib.util.module_from_spec(spec)
        sys.modules[modname] = mod
        spec.loader.exec_module(mod)
        return mod
    return importlib.import_module(path_or_module)


def load(spec: str, context: dict | None = None) -> Estimator:
    """Create an estimator from a spec.

    * ``oracle``, ``truecard``, ``feedback``... : a registered name
    * ``matcomp:rank=3``                        : a registered name with settings
    * ``file.py:Class`` / ``pkg.mod:Class``     : a user class
    * ``file.py:Class:arg``                     : a user class with an argument
    * ``saved:model.pkl``                       : an estimator saved with ``save()``
    """
    _load_builtins()
    context = context or {}
    name, _, rest = spec.partition(":")
    if name == "saved":
        est = Estimator.load_saved(rest)
        est.context = context
        est.pretrained = True       # the commands do not fit it again
        return est
    if name in _REGISTRY:
        return _REGISTRY[name](rest or None, context)
    if not rest:
        raise ValueError(f"unknown estimator {spec!r}; registered: {', '.join(registered())}")
    clsname, _, arg = rest.partition(":")
    mod = _import_from(name)
    cls = getattr(mod, clsname, None)
    if cls is None:
        raise ValueError(f"{name} has no class {clsname}")
    est = cls(arg or None, context)
    if not isinstance(est, Estimator):
        raise TypeError(f"{spec} is not a pgpi.Estimator")
    return est
