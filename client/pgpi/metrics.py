"""q-error evaluation on observed plan nodes.

Each estimator is asked the question of every usable node (a cardinality node
that was executed and not censored). When the estimator abstains, the planner
would have used the estimate of PostgreSQL, so that estimate is scored. Coverage
shows how often the estimator answered.
"""

from __future__ import annotations

import hashlib
import math
from typing import Callable, Iterable, Sequence

from .dataset import Dataset, Example
from .estimator import Estimator
from .request import Request, answer_rows, normalize_answer
from .schema import q_error


def summarize(values: Sequence[float]) -> dict:
    v = sorted(x for x in values if x is not None)
    n = len(v)
    if n == 0:
        return {"n": 0}

    def pct(p):
        k = (n - 1) * p
        lo, hi = math.floor(k), math.ceil(k)
        return v[lo] + (v[hi] - v[lo]) * (k - lo)

    return {"n": n, "gmean": math.exp(sum(math.log(x) for x in v) / n),
            "median": pct(0.5), "p90": pct(0.9), "p95": pct(0.95), "p99": pct(0.99), "max": v[-1]}


def bucket_rels(n: int) -> str:
    return str(n) if n < 5 else "5+"


class Split:
    """Split examples into train and test deterministically."""

    def __init__(self, test_fraction: float = 0.5, by: str = "queryid", seed: str = "pgpi"):
        self.test_fraction = test_fraction
        self.by = by
        self.seed = seed

    def is_test(self, ex: Example) -> bool:
        key = {"queryid": ex.queryid, "seqid": ex.seqid, "label": ex.label,
               "template": ex.request.template()}[self.by]
        h = hashlib.sha1(f"{self.seed}:{key}".encode()).digest()
        return int.from_bytes(h[:4], "big") / 2**32 < self.test_fraction


class ExampleView:
    """A dataset restricted to some examples. This is what ``fit`` sees."""

    def __init__(self, base: Dataset, keep: Callable[[Example], bool]):
        self._base = base
        self._keep = keep

    def examples(self, *args, **kw):
        return (ex for ex in self._base.examples(*args, **kw) if self._keep(ex))

    def __getattr__(self, name):
        return getattr(self._base, name)


#: a node counts as worse than PostgreSQL when its q-error is larger than that of PostgreSQL by this factor
WORSE_FACTOR = 1.1


def _plan_answers(est: Estimator, exs: list[Example], propagate: bool,
                  tree: dict | None = None, planner_inputs: bool = False) -> list[tuple[float, bool]]:
    """Return the value of the estimator for each example of one plan, as the planner would see it.

    With ``propagate``, a join is asked with the estimate of PostgreSQL
    recomputed from the answers of the estimator for the base relations
    (PostgreSQL builds join estimates from its inputs). An abstention falls back
    to this recomputed value, which is what the planner would use.

    With the node ``tree`` of the plan, a join also gets the rows of its two
    inputs, as the planner would have them. These are ``outer_rows`` and
    ``inner_rows`` (the answers of the estimator for the inputs) and
    ``outer_aliases``. A live join request has ``outer_rows``, ``inner_rows``
    and ``split`` in the same way.

    An input without an observed example gives no rows, and the join is not
    asked with its inputs. Such an input is a scan that cannot be used as a
    label, for example the censored inner side of a nested loop. With
    ``planner_inputs``, such an input counts with the estimate of PostgreSQL
    for it (the rows of the relation itself for a parameterized scan), as the
    join request of the planner would.
    """
    base_adj: dict[tuple, float] = {}
    out: dict[int, tuple[float, bool]] = {}
    node_est: dict[int, float] = {}          # rows the planner would use for a node's relations

    def ask(req):
        return answer_rows(req, normalize_answer(est.estimate(req)))

    if propagate:
        for i, ex in enumerate(exs):
            req = ex.request
            if req.n_rels != 1 or req.point not in ("base", "param_base"):
                continue
            r = req.rels[0]
            k = (req.plan_name, r.rti)
            if req.point == "base":
                v = ask(req)
                pg = ex.est_pg or 1.0
                base_adj[k] = (v / max(pg, 1e-9)) if v is not None else 1.0
            elif k not in base_adj and req.data.get("rel_rows"):
                # the base question the planner also asked about this relation
                own = [c for c in req.data.get("clauses", []) if c.get("rels") == r.alias]
                b = Request({"point": "base", "rels": [req.data["rels"][0]], "outer": [],
                             "clauses": own, "outer_clauses": [], "opaque": req.opaque,
                             "nonrel": req.nonrel, "jointype": 0, "plan_name": req.plan_name,
                             "pg": req.data["rel_rows"], "tuples": req.tuples})
                v = ask(b)
                base_adj[k] = (v / max(req.data["rel_rows"], 1e-9)) if v is not None else 1.0

    by_node = {ex.node_id: i for i, ex in enumerate(exs)}

    def card_node(nid):
        """Return the nearest node at or below nid that answers a cardinality question."""
        while nid is not None:
            if nid in by_node:
                return nid
            ch = tree["children"].get(nid, [])
            if len(ch) != 1 or tree["point"].get(nid):
                return None
            nid = ch[0][1]
        return None

    order = sorted(range(len(exs)), key=lambda i: (exs[i].request.n_rels, exs[i].request.point != "base"))
    for i in order:
        ex = exs[i]
        req = ex.request
        pg = ex.est_pg
        if propagate and req.point == "join" and pg is not None:
            adj = 1.0
            for r in req.rels:
                adj *= base_adj.get((req.plan_name, r.rti), 1.0)
            pg = pg * adj
            extra = {"pg": pg}
            if tree is not None:
                ch = tree["children"].get(ex.node_id, [])
                o = [n for role, n in ch if role == "outer"]
                n_ = [n for role, n in ch if role == "inner"]
                oc = card_node(o[0]) if o else None
                ic = card_node(n_[0]) if n_ else None
                if oc in node_est and ic in node_est:
                    extra.update(outer_rows=node_est[oc], inner_rows=node_est[ic],
                                 outer_aliases=" ".join(sorted(r.alias for r in exs[by_node[oc]].request.rels)))
                elif planner_inputs and o and n_:
                    po, pi = _planner_input(tree, o[0]), _planner_input(tree, n_[0])
                    ro = node_est.get(oc) if oc in node_est else (po[1] if po else None)
                    ri = node_est.get(ic) if ic in node_est else (pi[1] if pi else None)
                    if oc in node_est:
                        outer_al = " ".join(sorted(r.alias for r in exs[by_node[oc]].request.rels))
                    elif po:
                        outer_al = " ".join(sorted(po[0]))
                    elif ic in node_est or pi:
                        inner = ({r.alias for r in exs[by_node[ic]].request.rels} if ic in node_est else set(pi[0]))
                        outer_al = " ".join(sorted({r.alias for r in req.rels} - inner))
                    else:
                        outer_al = None
                    if ro is not None and ri is not None and outer_al:
                        extra.update(outer_rows=ro, inner_rows=ri, outer_aliases=outer_al)
            req = Request(dict(req.data, **extra))
        v = ask(req)
        out[i] = (v, True) if v is not None else (pg, False)
        val = out[i][0]
        if req.point == "param_base" and req.n_rels == 1 and req.data.get("rel_rows"):
            r = req.rels[0]                  # an inner input: the relation's own (unparameterized) rows
            val = req.data["rel_rows"] * base_adj.get((req.plan_name, r.rti), 1.0)
        if val is not None:
            node_est[ex.node_id] = val
    return [out[i] for i in range(len(exs))]


def plan_trees(dataset, seqids) -> dict[int, dict]:
    """Node trees of some plans: children (role, node_id) by parent, each node's point, and for
    cardinality nodes (aliases, PostgreSQL's rows as a join input) in "input"."""
    import json
    import pyarrow.compute as pc
    trees: dict[int, dict] = {}
    if not seqids:
        return trees
    t = dataset.nodes(["seqid", "node_id", "parent", "role", "point", "est_pg", "ce"],
                      pc.field("seqid").isin(list(seqids)))
    for r in t.to_pylist():
        tr = trees.setdefault(r["seqid"], {"children": {}, "point": {}, "input": {}})
        tr["point"][r["node_id"]] = r["point"]
        tr["children"].setdefault(r["parent"], []).append((r["role"], r["node_id"]))
        if r["point"] and r["ce"]:
            try:
                ce = json.loads(r["ce"])
            except ValueError:
                continue
            aliases = tuple(x.get("alias") for x in ce.get("rels", []))
            rows = ce.get("rel_rows") if r["point"] == "param_base" else r["est_pg"]
            if rows is not None and all(aliases):
                tr["input"][r["node_id"]] = (aliases, rows)
    return trees


def _planner_input(tree, nid):
    """(aliases, PostgreSQL's rows) of the cardinality node at or below nid along single children."""
    while nid is not None:
        if nid in tree.get("input", {}):
            return tree["input"][nid]
        ch = tree["children"].get(nid, [])
        if len(ch) != 1:
            return None
        nid = ch[0][1]
    return None


def evaluate(dataset: Dataset, estimators: Sequence[Estimator], *,
             points: Iterable[str] | None = None, labels: Iterable[str] | None = None,
             split: Split | None = None, fit: bool = True, propagate: bool = True,
             group_by: str = "point", planner_inputs: bool = False) -> dict:
    """Return the q-error of PostgreSQL and of each estimator, grouped by point or join size.

    With ``split``, estimators are fitted on the training part and scored on
    the test part. Without it, they are fitted and scored on everything
    (this is optimistic). ``propagate`` (default) scores join estimates the way
    the planner would form them from corrected inputs (see _plan_answers).
    ``planner_inputs`` also gives a join the estimates of PostgreSQL for inputs
    that were not observed.
    """
    if fit:
        train = ExampleView(dataset, (lambda ex: not split.is_test(ex)) if split else (lambda ex: True))
        for est in estimators:
            est.fit(train)

    names = ["postgres"] + [e.name for e in estimators]
    wanted = set(points) if points else None
    by_plan: dict[int, list[Example]] = {}
    for ex in dataset.examples(None, labels=labels):
        if split and not split.is_test(ex):
            continue
        by_plan.setdefault(ex.seqid, []).append(ex)

    groups: dict[str, dict[str, list[float]]] = {}
    answered: dict[str, int] = {n: 0 for n in names}
    worse: dict[str, int] = {n: 0 for n in names}     # nodes clearly worse than PostgreSQL
    total = 0
    trees = plan_trees(dataset, list(by_plan)) if propagate else {}
    for seqid, exs in by_plan.items():
        per_est = [_plan_answers(est, exs, propagate, trees.get(seqid), planner_inputs) for est in estimators]
        for i, ex in enumerate(exs):
            if wanted and ex.request.point not in wanted:
                continue
            total += 1
            g = ex.request.point if group_by == "point" else bucket_rels(ex.request.n_rels)
            bucket = groups.setdefault(g, {n: [] for n in names})
            q_pg = q_error(ex.est_pg, ex.true_rows)
            bucket["postgres"].append(q_pg)
            answered["postgres"] += 1
            for est, ans in zip(estimators, per_est):
                v, ok = ans[i]
                answered[est.name] += 1 if ok else 0
                q = q_error(v, ex.true_rows)
                bucket[est.name].append(q)
                if q_pg is not None and q is not None and q > q_pg * WORSE_FACTOR:
                    worse[est.name] += 1

    result = {"examples": total, "coverage": {n: (answered[n] / total if total else 0.0) for n in names},
              "worse_than_pg": {n: (worse[n] / total if total else 0.0) for n in names},
              "groups": {}}
    allv: dict[str, list[float]] = {n: [] for n in names}
    for g in sorted(groups):
        result["groups"][g] = {n: summarize(groups[g][n]) for n in names}
        for n in names:
            allv[n].extend(groups[g][n])
    result["groups"]["all"] = {n: summarize(allv[n]) for n in names}
    return result


def format_report(res: dict) -> str:
    lines = [f"examples: {res['examples']}",
             "coverage: " + ", ".join(f"{k} {v:.0%}" for k, v in res["coverage"].items())]
    if "worse_than_pg" in res:
        lines.append(f"worse than postgres (q-error x{WORSE_FACTOR}): " + ", ".join(
            f"{k} {v:.0%}" for k, v in res["worse_than_pg"].items() if k != "postgres"))
    lines.append("")
    hdr = f"{'group':<12}{'estimator':<14}{'n':>7}{'gmean':>10}{'median':>10}{'p90':>11}{'p99':>12}{'max':>13}"
    lines.append(hdr)
    lines.append("-" * len(hdr))
    for g, per in res["groups"].items():
        for name, s in per.items():
            if s.get("n", 0) == 0:
                continue
            lines.append(f"{g:<12}{name:<14}{s['n']:>7}{s['gmean']:>10.2f}{s['median']:>10.2f}"
                         f"{s['p90']:>11.2f}{s['p99']:>12.2f}{s['max']:>13.1f}")
    return "\n".join(lines)
