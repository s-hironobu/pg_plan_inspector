"""q-error of the estimates the planner actually used, in recorded runs.

The script reads a dataset. It does no fitting and needs no database.

For every usable plan node of the chosen labels, it compares ``est_plan`` with the
actual rows and prints the q-error. ``est_plan`` is what the planner used: the
answer of the estimator, or the estimate of PostgreSQL where the estimator
abstained. The nodes are grouped by label. With --by-point, they are grouped by
point and join size. The q-error columns of the benchmark reports summarize the
same values, with less detail.

For ceb_1a_continual (labels cc-c<k>-<runner>-<set>), --continual groups the nodes
by runner and set and shows the cycles side by side.

Run in client/:

    python3 tools/qerror_runs.py --data ../benchmark/data/ceb_1a/ds --like 'ceb-%'
    python3 tools/qerror_runs.py --data ../benchmark/data/ceb_1a_continual/train/ds --continual
    python3 tools/qerror_runs.py --data ../benchmark/data/ceb_1a_continual/train/ds \\
        --like 'cc-c5-%-Eu' --by-point
"""

from __future__ import annotations

import argparse
import collections
import fnmatch
import math
import os
import re
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from pgpi.dataset import Dataset  # noqa: E402
from pgpi.schema import q_error  # noqa: E402

CC = re.compile(r"^cc-c(\d+)-(.+)-(R|N|Es|Eu)$")


def summary(qs: list[float]) -> str:
    if not qs:
        return f"{0:>6} {'-':>7} {'-':>7} {'-':>9} {'-':>9}"
    qs = sorted(qs)
    g = math.exp(sum(math.log(q) for q in qs) / len(qs))
    return (f"{len(qs):>6} {g:7.2f} {statistics.median(qs):7.2f} {qs[int(0.9 * (len(qs) - 1))]:9.1f} "
            f"{qs[int(0.99 * (len(qs) - 1))]:9.1f}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--data", required=True)
    ap.add_argument("--like", default="%", help="labels, SQL LIKE pattern (default: all)")
    ap.add_argument("--by-point", action="store_true", help="also by point and join size")
    ap.add_argument("--continual", action="store_true", help="ceb_1a_continual: runner x set, cycles side by side")
    a = ap.parse_args()

    pat = a.like.replace("%", "*").replace("_", "?")
    groups = collections.defaultdict(list)
    for ex in Dataset(a.data).examples(None):
        lab = ex.label or ""
        if not fnmatch.fnmatchcase(lab, pat) or ex.est_plan is None:
            continue
        q = q_error(ex.est_plan, ex.true_rows)
        if q is None:
            continue
        req = ex.request
        point = req.point if req.point != "join" else f"join{req.n_rels}"
        groups[(lab, "all")].append(q)
        if a.by_point:
            groups[(lab, point)].append(q)

    if a.continual:
        cells = collections.defaultdict(dict)
        for (lab, point), qs in groups.items():
            m = CC.match(lab)
            if m and point == "all":
                cells[(m.group(2), m.group(3))][int(m.group(1))] = qs
        cycles = sorted({k for v in cells.values() for k in v})
        print("median q-error of the estimates the planner used (p90 in parentheses), per cycle\n")
        print(f"{'runner':<17} {'set':<4}" + "".join(f" {'c' + str(k):>15}" for k in cycles))
        order = {"R": 0, "N": 1, "Es": 2, "Eu": 3}
        for (runner, s) in sorted(cells, key=lambda x: (x[0] != "postgres", x[0], order[x[1]])):
            row = f"{runner:<17} {s:<4}"
            for k in cycles:
                qs = sorted(cells[(runner, s)].get(k, []))
                row += f" {statistics.median(qs):6.2f} ({qs[int(0.9 * (len(qs) - 1))]:6.1f})" if qs else f" {'-':>15}"
            print(row)
        return

    print(f"{'label':<32} {'point':<10} {'n':>6} {'gmean':>7} {'median':>7} {'p90':>9} {'p99':>9}")
    for (lab, point) in sorted(groups, key=lambda x: (x[0], x[1] != "all", x[1])):
        print(f"{lab:<32} {point:<10} {summary(groups[(lab, point)])}")


if __name__ == "__main__":
    main()
