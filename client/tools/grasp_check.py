"""grasp: find where a join estimate goes wrong, in the counts of the tables or in the sketches.

grasp estimates a join as  prod_t c(q_t) * prod_G overlap(G).
c(q_t) is the rows of each table under its own predicates (CardEst).
overlap(G) is the inner product of the learned count sketches of a join key group.

This script takes the join nodes of a chosen set of tables. For each distinct
question, it prints the estimate of grasp for each factor, next to the actual
value where the dataset has it:

    c(alias)   The count of grasp for the table under its predicates. The actual
               value is the rows of the same scan, if a base node with the same
               predicates was recorded (any label).
    overlap    The total divided by the counts (for one key group). The actual
               value is implied by the actual counts:
               actual join rows / prod_t actual c(t)  (shown when every c(t) is known)

With in_split, c(alias) is the sum over the values of its IN lists.

At the end, the script prints the q-error of the estimate by predicate value.
A value that the model cannot tell from the others shows up as a constant estimate.

Fitting takes as long as for the benchmarks. --save keeps the fitted model, and
--load reuses it. A model saved before a change of grasp.py must be fitted again.
With --fit-only-tables, the model is fitted on the nodes of these tables only.
Run in client/:

    python3 tools/grasp_check.py --data ../benchmark/data/ceb_1a/ds --fit-label ceb-train \\
        --tables imdb.kind_type,imdb.title --save /tmp/grasp.pkl
    python3 tools/grasp_check.py --data ../benchmark/data/ceb_1a/ds --load /tmp/grasp.pkl \\
        --tables imdb.kind_type,imdb.title --label ceb-grasp
"""

from __future__ import annotations

import argparse
import collections
import math
import os
import pickle
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import pgpi  # noqa: E402
from pgpi.metrics import ExampleView  # noqa: E402
from pgpi.predicates import decompose  # noqa: E402


def restrictions(req, alias) -> tuple[str, ...]:
    return tuple(sorted(c.sql or "<opaque>" for c in req.clauses if len(c.rels) == 1 and c.rels[0] == alias))


def qe(est, true) -> float:
    e, t = max(est, 1.0), max(true, 1.0)
    return e / t if e >= t else -(t / e)


def fmt(x) -> str:
    if x is None:
        return "-"
    return f"{x:.3g}"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--data", required=True)
    ap.add_argument("--plugin", default="grasp:epochs=100,restarts=1",
                    help="grasp spec (default: the benchmarks' grasp:epochs=100,restarts=1)")
    ap.add_argument("--fit-label", action="append", help="labels to fit on (default: all)")
    ap.add_argument("--label", action="append", help="labels whose nodes are checked (default: all)")
    ap.add_argument("--tables", required=True, help="the join's tables, comma-separated (e.g. imdb.kind_type,imdb.title)")
    ap.add_argument("--fit-only-tables", action="store_true",
                    help="fit only on the nodes whose tables are among --tables (the join and its scans), "
                         "to see whether the model can learn them in isolation")
    ap.add_argument("--save", help="save the fitted model to this file")
    ap.add_argument("--load", help="load a model saved with --save instead of fitting")
    ap.add_argument("--top", type=int, default=15, help="questions listed, worst first (default 15)")
    a = ap.parse_args()

    ds = pgpi.Dataset(a.data)
    if a.load:
        with open(a.load, "rb") as f:
            est = pickle.load(f)
    else:
        est = pgpi.load(a.plugin)
        want_set = set(a.tables.split(","))
        keep = (lambda ex: (not a.fit_label or ex.label in a.fit_label)
                and (not a.fit_only_tables or set(ex.request.tables) <= want_set))
        est.fit(ExampleView(ds, keep))
        if a.save:
            with open(a.save, "wb") as f:
                pickle.dump(est, f)
    import torch

    want = sorted(a.tables.split(","))
    # the actual counts of single-table scans, by table and predicates, from any label
    base_true: dict[tuple, list[float]] = collections.defaultdict(list)
    for ex in ds.examples(["base"]):
        r = ex.request
        base_true[(r.rels[0].rel, restrictions(r, r.rels[0].alias))].append(ex.true_rows)

    seen: dict[str, dict] = {}
    for ex in ds.examples(["join"], labels=a.label):
        req = ex.request
        if sorted(req.tables) != want or req.key() in seen:
            continue
        comp, why = decompose(req)
        if comp is None or not est._supported(comp):
            continue
        with torch.no_grad():
            # With in_split, the question is the sum of its variants (one per IN value).
            # c(alias) is summed over the variants. The overlap is what remains of the total.
            variants = est._variants(comp)
            insts, bases = [], []
            for v in variants:
                bases.append(len(insts))
                insts.extend(tq for _, tq in v.aliases())
            log_c, log_v = est._forward(insts)
            total = sum(float(torch.exp(est._compose(v, log_c, log_v, b))) for v, b in zip(variants, bases))
            per_alias = collections.defaultdict(dict)
            for v, b in zip(variants, bases):
                for j, (al, tq) in enumerate(v.aliases()):
                    if al in dict(v.targets):
                        per_alias[al][tq] = math.exp(float(log_c[b + j]))
            counts = {}
            for al, tq in comp.targets:
                t = base_true.get((tq.table, restrictions(req, al)))
                counts[al] = (sum(per_alias[al].values()), statistics.median(t) if t else None, tq.table)
            prod_c = math.prod(c[0] for c in counts.values())
            overlaps = []
            if len(comp.groups) == 1:
                g = comp.groups[0]
                overlaps.append((" = ".join(f"{al}.{col}" for al, col in g), total / prod_c if prod_c else float("nan")))
            else:
                for g in comp.groups:
                    overlaps.append((" = ".join(f"{al}.{col}" for al, col in g), float("nan")))
        actual_c = [c[1] for c in counts.values()]
        implied = ex.true_rows / math.prod(actual_c) if all(x for x in actual_c) else None
        seen[req.key()] = {"ex": ex, "req": req, "total": total, "counts": counts, "overlaps": overlaps,
                           "implied": implied}

    if not seen:
        sys.exit(f"no join nodes of {want} in the chosen labels")
    rows = sorted(seen.values(), key=lambda r: -abs(qe(r["total"], r["ex"].true_rows)))
    print(f"{len(rows)} distinct questions on {' '.join(want)}; worst first\n")
    for r in rows[:a.top]:
        ex, req = r["ex"], r["req"]
        print(f"q {qe(r['total'], ex.true_rows):+.0f}  grasp {fmt(r['total'])}  actual {fmt(ex.true_rows)}  "
              f"postgres {fmt(ex.est_pg)}   [{ex.label}]")
        for al, (c, t, table) in r["counts"].items():
            print(f"     c({al}) grasp {fmt(c)}  actual {fmt(t)}   {' AND '.join(restrictions(req, al))[:150]}")
        for name, ov in r["overlaps"]:
            print(f"     overlap {name}: grasp {fmt(ov)}  actual {fmt(r['implied']) if len(r['overlaps']) == 1 else '-'}")

    # by predicate value. A constant estimate across values means that the model cannot tell them apart.
    print("\nby restriction set: n, median actual, median grasp, median q-error")
    by = collections.defaultdict(list)
    for r in rows:
        key = " | ".join(" AND ".join(restrictions(r["req"], al)) for al, _ in sorted(r["counts"].items()))
        by[key].append(r)
    for k, rs in sorted(by.items(), key=lambda kv: -len(kv[1]))[:a.top]:
        print(f"   {len(rs):>3}  {fmt(statistics.median(x['ex'].true_rows for x in rs)):>9}  "
              f"{fmt(statistics.median(x['total'] for x in rs)):>9}  "
              f"{statistics.median(qe(x['total'], x['ex'].true_rows) for x in rs):+8.1f}   {k[:150]}")


if __name__ == "__main__":
    main()
