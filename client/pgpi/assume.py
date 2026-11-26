"""Cardinality-model assumptions that can be switched, as in SQL Server.

SQL Server's cardinality estimator has named model assumptions. A query can
switch them with ``OPTION (USE HINT (...))``. Since SQL Server 2022, "CE feedback"
switches them automatically per query when executions show that the default is
wrong. Two of them are about combining selectivities, which is also what
PostgreSQL's planner does, with fixed choices:

Correlation of conjunctive filters on one table (selectivities s_i)

  independence   s_1 * s_2 * ... * s_n                    PostgreSQL; SQL Server CE 70
  partial        exponential backoff over the four most selective,
                 sorted ascending:  s_1 * s_2^(1/2) * s_3^(1/4) * s_4^(1/8)
                                                          SQL Server CE 120+ default
  full           min(s_i)                                 ASSUME_MIN_SELECTIVITY_FOR_FILTER_ESTIMATES

Containment of an equi-join between filtered inputs

  base           join selectivity from the base distributions, then the
                 filters are factored in (independently)  PostgreSQL; SQL Server CE 120+
  simple         the filters are applied to each input's distribution
                 first, then the join selectivity comes from the filtered
                 distributions: the join predicate depends on the filters
                                                          ASSUME_JOIN_PREDICATE_DEPENDS_ON_FILTERS; CE 70

For an equi-join a.x = b.y with d distinct values on each side:

  base:    sel = 1 / max(d_a, d_b)
  simple:  sel = 1 / max(d'_a, d'_b),   d' = distinct values left after the filters

  d' = d * s_key * (1 - (1 - s_other)^(N / d))

where s_key is the selectivity of the filters on the join column itself (a
range on the key keeps that share of its values), and s_other is the combined
selectivity of the filters on the other columns (a random subset of the rows
keeps fewer distinct values: the classic expected-distinct formula).
PostgreSQL's estimate behaves like ``base``. So ``simple`` is applied as the
factor  max(d_a, d_b) / max(d'_a, d'_b)  on the value of PostgreSQL.
"""

from __future__ import annotations

from dataclasses import dataclass

CORRELATION = ("independence", "partial", "full")
CONTAINMENT = ("base", "simple")


@dataclass(frozen=True)
class Model:
    correlation: str = "independence"
    containment: str = "base"

    def __post_init__(self):
        if self.correlation not in CORRELATION:
            raise ValueError(f"correlation must be one of {CORRELATION}")
        if self.containment not in CONTAINMENT:
            raise ValueError(f"containment must be one of {CONTAINMENT}")

    @property
    def name(self) -> str:
        return f"{self.correlation}/{self.containment}"

    @staticmethod
    def parse(name: str) -> "Model":
        c, _, j = name.partition("/")
        return Model(c or "independence", j or "base")

    def with_(self, **kw) -> "Model":
        d = {"correlation": self.correlation, "containment": self.containment}
        d.update(kw)
        return Model(**d)


#: PostgreSQL's own behaviour
POSTGRES = Model("independence", "base")

#: SQL Server's USE HINT names (and the CE versions they select)
HINTS = {
    "ASSUME_FULL_INDEPENDENCE_FOR_FILTER_ESTIMATES": {"correlation": "independence"},
    "ASSUME_PARTIAL_CORRELATION_FOR_FILTER_ESTIMATES": {"correlation": "partial"},
    "ASSUME_MIN_SELECTIVITY_FOR_FILTER_ESTIMATES": {"correlation": "full"},
    "ASSUME_JOIN_PREDICATE_DEPENDS_ON_FILTERS": {"containment": "simple"},
    "FORCE_LEGACY_CARDINALITY_ESTIMATION": {"correlation": "independence", "containment": "simple"},
    "FORCE_DEFAULT_CARDINALITY_ESTIMATION": {"correlation": "partial", "containment": "base"},
}


def parse_hints(text: str | None, base: Model) -> tuple[Model, bool]:
    """Apply hints (SQL Server names, or correlation=... / containment=...) to a model.

    Return (model, whether any hint was recognized). Unknown words are ignored,
    so pgpi.hints can carry hints for other estimators too.
    """
    if not text:
        return base, False
    kw: dict[str, str] = {}
    for tok in text.replace(",", " ").replace("(", " ").replace(")", " ").replace("'", " ").split():
        t = tok.strip()
        if t.upper() in HINTS:
            kw.update(HINTS[t.upper()])
        elif "=" in t:
            k, v = t.split("=", 1)
            k, v = k.lower(), v.lower()
            if k == "correlation" and v in CORRELATION or k == "containment" and v in CONTAINMENT:
                kw[k] = v
    return (base.with_(**kw), True) if kw else (base, False)


def combine(sels: list[float], correlation: str) -> float:
    """Return the selectivity of a conjunction of filters on different columns of one table."""
    s = sorted(max(0.0, min(1.0, x)) for x in sels)
    if not s:
        return 1.0
    if correlation == "independence":
        out = 1.0
        for x in s:
            out *= x
        return out
    if correlation == "full":
        return s[0]
    out = 1.0                                   # partial: exponential backoff
    for k, x in enumerate(s[:4]):
        out *= x ** (1.0 / (2 ** k))
    return out


def filtered_distinct(nd: float, n: float, s_key: float, s_other: float) -> float:
    """Return the distinct values of a join column left after the filters (see the module docstring)."""
    nd = max(min(nd, n), 1.0)
    d = nd * max(min(s_key, 1.0), 0.0)
    if s_other >= 1.0:
        return max(d, 1e-9)
    return max(d * (1.0 - (1.0 - max(s_other, 0.0)) ** (n / nd)), 1e-9)


@dataclass
class JoinSide:
    nd: float          # distinct values of the join column (whole table)
    n: float           # rows of the table
    s_key: float       # selectivity of the filters on the join column
    s_other: float     # combined selectivity of the other filters


def containment_factor(a: JoinSide, b: JoinSide) -> float:
    """Return the factor simple / base for one equi-join clause."""
    base = max(min(a.nd, a.n), min(b.nd, b.n), 1.0)
    simple = max(filtered_distinct(a.nd, a.n, a.s_key, a.s_other),
                 filtered_distinct(b.nd, b.n, b.s_key, b.s_other), 1e-9)
    return base / simple
