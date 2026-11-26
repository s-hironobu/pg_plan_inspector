"""A column's distribution as bins, from PostgreSQL's statistics.

The methods of the blog need the marginal distribution of each column as a
vector. PostgreSQL keeps it in pg_statistic. pgpi captures it with every new
statistics version (the ``stats`` table of the dataset). A column becomes

    [ one bin per MCV ] [ one bin per histogram interval ] [ NULL ]

* an MCV bin has the frequency of the MCV.
* the histogram intervals share the rest equally (equi-depth histogram).
  Without a histogram, the rest is a single bin.
* NULL has null_frac.

A predicate on the column becomes a coverage vector: the fraction of each bin
that satisfies it. PostgreSQL computes a selectivity in the same way. It uses
MCVs exactly, linear interpolation inside a histogram bin, and shares the rest
among the distinct values that are not MCVs. So  coverage . freq  is close to
the estimate of PostgreSQL for that predicate.
"""

from __future__ import annotations

import bisect
import json
from dataclasses import dataclass

from .predicates import NUMERIC_TYPES, Pred, to_number

STATISTIC_KIND_MCV = 1
STATISTIC_KIND_HISTOGRAM = 2
SUPPORTED_OPS = ("=", "<>", "<", "<=", ">", ">=", "in", "isnull", "notnull")


@dataclass
class ColumnBins:
    table: str
    column: str
    type: str
    mcv: list            # MCV values (Python values: float for numeric types, str otherwise)
    hist: list           # histogram bounds (same kind of values), [] if none
    freqs: list          # bin frequencies, summing to 1
    n_distinct_rest: float   # distinct non-null values that are not MCVs
    numeric: bool
    has_null: bool = False   # the last bin is NULL

    @property
    def n_bins(self) -> int:
        return len(self.freqs)

    def label(self, i: int) -> str:
        k = len(self.mcv)
        if i < k:
            return f"{self.mcv[i]}"
        if self.has_null and i == self.n_bins - 1:
            return "NULL"
        if self.hist:
            j = i - k
            return f"[{self.hist[j]},{self.hist[j + 1]}]"
        return "rest"

    # ---------------------------------------------------------- coverage

    def value(self, raw: str | None, vtype: str | None):
        if raw is None:
            return None
        if self.numeric:
            return to_number(raw, vtype or self.type)
        return raw

    def coverage(self, preds: list[Pred]) -> list[float] | None:
        """Return the fraction of each bin satisfying all predicates (on this column), or None."""
        if not preds or any(p.op not in SUPPORTED_OPS for p in preds):
            return None
        eqs, neqs, lo, hi = None, [], None, None     # lo/hi: (value, inclusive)
        null_wanted = None
        for p in preds:
            if p.op in ("isnull", "notnull"):
                null_wanted = p.op == "isnull"
                continue
            if p.op == "in":
                vals = [self.value(x, p.vtype.replace("[]", "") if p.vtype else None)
                        for x in _array_items(p.value)]
            else:
                vals = [self.value(p.value, p.vtype)]
            if any(v is None for v in vals):
                return None
            if p.op in ("=", "in"):
                s = set(vals)
                eqs = s if eqs is None else (eqs & s)
            elif p.op == "<>":
                neqs.append(vals[0])
            else:
                v = vals[0]                # text: Python string order (approximate)
                if p.op in ("<", "<="):
                    c = (v, p.op == "<=")
                    if hi is None or c[0] < hi[0] or (c[0] == hi[0] and not c[1]):
                        hi = c
                else:
                    c = (v, p.op == ">=")
                    if lo is None or c[0] > lo[0] or (c[0] == lo[0] and not c[1]):
                        lo = c

        def ok(v):
            if eqs is not None and v not in eqs:
                return False
            if v in neqs:
                return False
            if lo is not None and (v < lo[0] or (v == lo[0] and not lo[1])):
                return False
            if hi is not None and (v > hi[0] or (v == hi[0] and not hi[1])):
                return False
            return True

        k = len(self.mcv)
        cov = [0.0] * self.n_bins
        if null_wanted:
            if self.has_null and eqs is None and lo is None and hi is None:
                cov[-1] = 1.0
            return cov
        for i, v in enumerate(self.mcv):
            cov[i] = 1.0 if ok(v) else 0.0
        rest_bins = list(range(k, self.n_bins - (1 if self.has_null else 0)))
        if not rest_bins:
            return cov
        per_value = self._rest_mass() / max(self.n_distinct_rest, 1.0)   # PostgreSQL's eqsel for a non-MCV
        mcv_set = set(self.mcv)
        if eqs is not None:
            for v in eqs:
                if v in mcv_set or not ok(v):
                    continue
                b = self._bin_of(v, rest_bins)
                cov[b] = min(1.0, cov[b] + per_value / max(self.freqs[b], 1e-300))
            return cov
        for b in rest_bins:
            cov[b] = self._fraction_in(b - k, lo, hi)
        for v in neqs:
            if v in mcv_set:
                continue
            b = self._bin_of(v, rest_bins)
            cov[b] = max(0.0, cov[b] - per_value / max(self.freqs[b], 1e-300))
        return cov

    def _rest_mass(self) -> float:
        k = len(self.mcv)
        end = self.n_bins - (1 if self.has_null else 0)
        return float(sum(self.freqs[k:end]))

    def _bin_of(self, v, rest_bins) -> int:
        if not self.hist:
            return rest_bins[0]
        j = bisect.bisect_right(self.hist, v) - 1
        return rest_bins[min(max(j, 0), len(rest_bins) - 1)]

    def _fraction_in(self, j: int, lo, hi) -> float:
        """Return the fraction of histogram interval j (or the single rest bin) inside (lo, hi)."""
        if not self.hist:
            # no histogram: use the defaults of PostgreSQL (DEFAULT_INEQ_SEL, DEFAULT_RANGE_INEQ_SEL)
            if lo is None and hi is None:
                return 1.0
            return 0.005 if (lo is not None and hi is not None) else 1.0 / 3.0
        a, b = self.hist[j], self.hist[j + 1]
        if not self.numeric:
            inside_lo = lo is None or a >= lo[0]
            inside_hi = hi is None or b <= hi[0]
            out = (lo is not None and b < lo[0]) or (hi is not None and a > hi[0])
            return 0.0 if out else 1.0 if (inside_lo and inside_hi) else 0.5
        x0 = a if lo is None else max(a, lo[0])
        x1 = b if hi is None else min(b, hi[0])
        if b <= a:
            return 1.0 if x0 <= a <= x1 else 0.0
        return max(0.0, min(1.0, (x1 - x0) / (b - a)))


def _array_items(lit: str | None) -> list[str]:
    s = (lit or "").strip()
    if s.startswith("{") and s.endswith("}"):
        s = s[1:-1]
    out = []
    for x in s.split(","):
        x = x.strip()
        if len(x) >= 2 and x[0] == '"' and x[-1] == '"':
            x = x[1:-1].replace('\\"', '"')
        if x:
            out.append(x)
    return out


def column_bins(table: str, stats: dict, column: str) -> ColumnBins | None:
    """Return the bins of one column from a captured statistics record (pgpi.stats)."""
    col = next((c for c in stats.get("columns", []) if c.get("attname") == column and not c.get("inh")), None)
    if col is None:
        return None
    typ = col.get("type") or ""
    numeric = typ.split("(")[0].strip() in NUMERIC_TYPES

    def conv(x):
        return to_number(x, typ) if numeric else x

    mcv, mcf, hist = [], [], []
    for s in col.get("slots", []):
        if s.get("kind") == STATISTIC_KIND_MCV:
            mcv = [conv(x) for x in s.get("values", [])]
            mcf = [float(x) for x in s.get("numbers", [])]
        elif s.get("kind") == STATISTIC_KIND_HISTOGRAM:
            hist = [conv(x) for x in s.get("values", [])]
    if any(v is None for v in mcv + hist):
        return None
    null_frac = float(col.get("null_frac") or 0.0)
    rest = max(0.0, 1.0 - null_frac - sum(mcf))
    freqs = list(mcf)
    if len(hist) >= 2:
        freqs += [rest / (len(hist) - 1)] * (len(hist) - 1)
    elif rest > 1e-9:
        hist = []
        freqs.append(rest)
    has_null = null_frac > 0
    if has_null:
        freqs.append(null_frac)
    tot = sum(freqs) or 1.0
    freqs = [f / tot for f in freqs]
    nd = float(col.get("n_distinct") or 0.0)
    if nd < 0:
        nd = -nd * float(stats.get("reltuples") or 0.0)
    return ColumnBins(table, column, typ, mcv, hist if len(hist) >= 2 else [], freqs,
                      max(nd - len(mcv), 1.0), numeric, has_null)


class StatsCatalog:
    """The latest captured statistics of each table in a dataset."""

    def __init__(self, dataset):
        self.by_rel: dict[str, dict] = {}
        try:
            rows = dataset.stats(["rel", "captured_at", "stats"]).to_pylist()
        except Exception:      # a dataset without statistics
            rows = []
        rows.sort(key=lambda r: str(r["captured_at"]))
        for r in rows:
            self.by_rel[r["rel"]] = json.loads(r["stats"]) if isinstance(r["stats"], str) else r["stats"]

    def bins(self, table: str, column: str) -> ColumnBins | None:
        st = self.by_rel.get(table)
        return column_bins(table, st, column) if st else None
