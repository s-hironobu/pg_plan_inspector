"""q-error of the answers of an estimator, by point and by the source of the answer.

The script fits one plugin on the examples of some labels. It asks the plugin
every node of other labels, one by one (as ``qerror --no-propagate`` does). Then
it groups the nodes by point and by the ``source`` that the plugin gave
(``litecard-h3``, ``litecard-bias``, ``nngp``, ...). The source is "abstain" when
the plugin gave no answer and the estimate of PostgreSQL stands.

For each group, it prints the number of nodes and the q-error of the plugin
next to the q-error of PostgreSQL on the same nodes.

For litecard, the abstained nodes are also broken down by the reason. The script
compares the largest history of their pattern at any level with the thresholds
beta. Either no pattern was seen at all, or it was seen too rarely.

Run in client/:

    python3 tools/qerror_by_source.py --data DS --plugin litecard \\
        --fit-label ceb-train --label ceb-postgres [--dsn DSN]
"""

from __future__ import annotations

import argparse
import collections
import math
import os
import re
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import pgpi  # noqa: E402
from pgpi.dataset import Dataset  # noqa: E402
from pgpi.metrics import ExampleView  # noqa: E402
from pgpi.request import answer_rows, normalize_answer  # noqa: E402
from pgpi.schema import q_error  # noqa: E402


def stats(qs: list[float]) -> str:
    if not qs:
        return f"{'-':>8} {'-':>8} {'-':>9}"
    qs = sorted(qs)
    g = math.exp(sum(math.log(q) for q in qs) / len(qs))
    return f"{g:8.2f} {statistics.median(qs):8.2f} {qs[int(0.9 * (len(qs) - 1))]:9.1f}"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--data", required=True)
    ap.add_argument("--plugin", required=True)
    ap.add_argument("--fit-label", action="append", required=True)
    ap.add_argument("--label", action="append", required=True)
    ap.add_argument("--dsn", default="")
    ap.add_argument("--by-size", action="store_true", help="also split joins by their number of relations")
    a = ap.parse_args()

    ds = Dataset(a.data)
    est = pgpi.load(a.plugin, {"dsn": a.dsn})
    est.fit(ExampleView(ds, lambda ex: ex.label in a.fit_label))

    groups = collections.defaultdict(lambda: ([], []))          # (point, source) -> (q of plugin, q of pg)
    abstained = []
    total = collections.Counter()
    for ex in ds.examples(None, labels=a.label):
        req = ex.request
        if not ex.true_rows and ex.true_rows != 0:
            continue
        ans = normalize_answer(est.estimate(req))
        v = answer_rows(req, ans)
        src = re.sub(r"\s*\(.*\)$", "", ans.get("source", a.plugin)) if v is not None else "abstain"
        point = req.point
        if a.by_size and point == "join":
            point = f"join{req.n_rels}"
        q_pg = q_error(ex.est_pg, ex.true_rows)
        q = q_error(v, ex.true_rows) if v is not None else q_pg
        if q is None or q_pg is None:
            continue
        groups[(point, src)][0].append(q)
        groups[(point, src)][1].append(q_pg)
        total[point] += 1
        if v is None:
            abstained.append((point, req))

    print(f"{a.plugin}: fitted on {', '.join(a.fit_label)}, scored on {', '.join(a.label)}; "
          f"each node asked alone; q-error of the answer and of PostgreSQL's estimate on the same nodes\n")
    print(f"{'point':<11} {'source':<16} {'n':>6} {'share':>6}   {'gmean':>8} {'median':>8} {'p90':>9}"
          f"   | postgres {'gmean':>8} {'median':>8} {'p90':>9}")
    for (point, src) in sorted(groups):
        q, qp = groups[(point, src)]
        print(f"{point:<11} {src:<16} {len(q):>6} {len(q) / total[point]:6.0%}   {stats(q)}   |          {stats(qp)}")

    if a.plugin.split(":")[0] == "litecard" and abstained:
        from pgpi.estimator_plugins import litecard as lc
        print("\nlitecard, abstained nodes: the largest history of their pattern at any level "
              f"(beta: H3 {lc.BETA[3]}, H2 {lc.BETA[2]}, H1 {lc.BETA[1]})")
        why = collections.defaultdict(collections.Counter)
        for point, req in abstained:
            d = lc.describe(req, est.stats)
            if d is None:
                why[point]["not described (clauselist)"] += 1
                continue
            keys, _ = d
            sizes = {lv: len(est.buckets[(lv, keys[lv])][1]) if (lv, keys[lv]) in est.buckets else 0
                     for lv in (1, 2, 3)}
            if not any(sizes.values()):
                why[point]["pattern never seen at any level"] += 1
            else:
                best = max(sizes.values())
                bucket = "1-4" if best < 5 else "5-9" if best < 10 else "10-49" if best < 50 else "50-99"
                why[point][f"seen, too rarely (largest bucket {bucket})"] += 1
        for point in sorted(why):
            for reason, n in why[point].most_common():
                print(f"   {point:<11} {reason:<45} {n:>6}")


if __name__ == "__main__":
    main()
