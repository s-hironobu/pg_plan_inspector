"""slow_queries: list the queries that an estimator made slower, and show where its estimates were wrong.

The script reads a dataset. It does no fitting and needs no database.

It takes two labels of the same queries: the label of the estimator
(``--estimator-label``, e.g. ``ceb-grasp``) and the label of the baseline
(``--baseline-label``, by default the same prefix with ``postgres``). For each
query, it takes the median execution time under each label. It lists the queries
with the largest excess over the baseline. For each of them, it prints both plans
as trees. Every cardinality node shows the estimate that the planner used, the
actual rows and the q-error (``v`` for an underestimate, ``^`` for an
overestimate). The worst estimates of the plan of the estimator follow.

Run in client/:

    python3 tools/slow_queries.py --data ../benchmark/data/ceb_1a/ds --estimator-label ceb-grasp
    python3 tools/slow_queries.py --data ../benchmark/data/ceb_1a/ds --estimator-label ceb-grasp \\
        --top 3 --no-base
"""

from __future__ import annotations

import argparse
import collections
import os
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import pyarrow.compute as pc  # noqa: E402

from pgpi.dataset import Dataset  # noqa: E402

NODE_COLS = ["seqid", "node_id", "parent", "type", "tables", "point", "nloops",
             "est_plan", "true_rows", "censored"]


def runs(ds: Dataset, label: str) -> dict[str, list[dict]]:
    """Return the runs of a label, by query text. A run has seqid, execution_ms (None if not complete) and planid."""
    t = ds.plans(["seqid", "label", "query", "execution_ms", "status", "planid"],
                 pc.field("label") == label)
    out = collections.defaultdict(list)
    for r in t.to_pylist():
        q = r["query"] or ""
        if "pgpi_log_flush" in q:
            continue
        ms = r["execution_ms"] if r["status"] == "complete" else None
        out[q.strip()].append({"seqid": r["seqid"], "ms": ms, "planid": r["planid"], "status": r["status"]})
    return out


def median_run(rs: list[dict], timeout_ms: float) -> tuple[float, dict]:
    """Return the median time and the run closest to it. An unfinished run counts as timeout_ms."""
    times = [r["ms"] if r["ms"] is not None else timeout_ms for r in rs]
    med = statistics.median(times)
    i = min(range(len(rs)), key=lambda k: abs(times[k] - med))
    return med, rs[i]


def qerr(est, true) -> tuple[float, str] | None:
    if est is None or true is None:
        return None
    e, t = max(float(est), 1.0), max(float(true), 1.0)
    return (t / e, "v") if t >= e else (e / t, "^")


def fmt_rows(x) -> str:
    if x is None:
        return "-"
    x = float(x)
    return f"{x:.0f}" if x < 1e6 else f"{x:.2e}"


def plan_nodes(ds: Dataset, seqid: int) -> list[dict]:
    return ds.nodes(NODE_COLS, pc.field("seqid") == seqid).to_pylist()


def print_tree(nodes: list[dict], indent: str = "      ") -> None:
    kids = collections.defaultdict(list)
    for n in nodes:
        kids[n["parent"]].append(n)
    for v in kids.values():
        v.sort(key=lambda n: n["node_id"])

    def walk(n, depth):
        line = f"{indent}{'  ' * depth}{n['type']}"
        if n["tables"]:
            line += f" [{' '.join(n['tables'])}]"
        if n["point"]:
            q = qerr(n["est_plan"], n["true_rows"])
            loops = f" x{n['nloops']:g}" if (n["nloops"] or 1) > 1 else ""
            mark = f"  q {q[0]:.0f}{q[1]}" if q and q[0] >= 2 else ""
            line += f"  est {fmt_rows(n['est_plan'])}  actual {fmt_rows(n['true_rows'])}{loops}{mark}"
            if n["censored"]:
                line += " (stopped early)"
        print(line)
        for c in kids.get(n["node_id"], []):
            walk(c, depth + 1)

    ids = {n["node_id"] for n in nodes}
    for root in sorted((n for n in nodes if n["parent"] not in ids), key=lambda n: n["node_id"]):
        walk(root, 0)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--data", required=True)
    ap.add_argument("--estimator-label", required=True, help="label of the estimator's runs, e.g. ceb-grasp")
    ap.add_argument("--baseline-label", help="label of the baseline's runs (default: <prefix>-postgres)")
    ap.add_argument("--top", type=int, default=5, help="number of queries (default 5)")
    ap.add_argument("--min-ms", type=float, default=100.0, help="smallest excess shown, ms (default 100)")
    ap.add_argument("--nodes", type=int, default=5, help="worst estimates listed per query (default 5)")
    ap.add_argument("--timeout-ms", type=float, default=60000.0,
                    help="time counted for a run that did not complete (default 60000)")
    ap.add_argument("--no-base", action="store_true", help="do not print the baseline's plan")
    a = ap.parse_args()

    base = a.baseline_label or a.estimator_label.rsplit("-", 1)[0] + "-postgres"
    ds = Dataset(a.data)
    m_runs, b_runs = runs(ds, a.estimator_label), runs(ds, base)
    if not m_runs or not b_runs:
        sys.exit(f"no runs for {a.estimator_label if not m_runs else base} in {a.data}")

    rows = []
    for q in m_runs.keys() & b_runs.keys():
        mm, mr = median_run(m_runs[q], a.timeout_ms)
        bm, br = median_run(b_runs[q], a.timeout_ms)
        rows.append((mm - bm, mm, bm, q, mr, br))
    rows.sort(key=lambda r: -r[0])
    tot_m, tot_b = sum(r[1] for r in rows), sum(r[2] for r in rows)
    slower = [r for r in rows if r[0] >= a.min_ms]
    print(f"{a.estimator_label} against {base}: {len(rows)} queries, total {tot_m:.0f} ms against {tot_b:.0f} ms "
          f"({tot_m / tot_b:.2f}); {len(slower)} queries at least {a.min_ms:.0f} ms slower, "
          f"together {sum(r[0] for r in slower):.0f} ms of the difference")

    for k, (d, mm, bm, q, mr, br) in enumerate(slower[:a.top], 1):
        same = "the same plan" if mr["planid"] == br["planid"] else "a different plan"
        print(f"\n== {k}. +{d:.0f} ms: {mm:.0f} ms against {bm:.0f} ms ({mm / max(bm, 1e-9):.1f}x), {same}"
              + ("" if mr["ms"] is not None else f" ({mr['status']}, counted {a.timeout_ms:.0f} ms)"))
        print("   " + " ".join(q.split())[:300])
        mn = plan_nodes(ds, mr["seqid"])
        print(f"   plan with {a.estimator_label} (seqid {mr['seqid']}):")
        print_tree(mn)
        if not a.no_base:
            print(f"   plan with {base} (seqid {br['seqid']}):")
            print_tree(plan_nodes(ds, br["seqid"]))
        worst = []
        for n in mn:
            if n["point"]:
                e = qerr(n["est_plan"], n["true_rows"])
                if e:
                    worst.append((e[0], e[1], n))
        worst.sort(key=lambda w: -w[0])
        print(f"   worst estimates with {a.estimator_label}:")
        for qv, s, n in worst[:a.nodes]:
            kind = "under" if s == "v" else "over"
            print(f"      q {qv:9.1f} {kind:<5} {n['type']} [{' '.join(n['tables'] or [])}]  "
                  f"est {fmt_rows(n['est_plan'])}  actual {fmt_rows(n['true_rows'])}")


if __name__ == "__main__":
    main()
