"""Command line: ``pgpi <command> ...`` (or ``python -m pgpi``)."""

from __future__ import annotations

import argparse
import json
import sys

from . import __version__


def _estimators(specs, ctx):
    from .estimator import load
    return [load(s, ctx) for s in specs]


def cmd_serve(a):
    from .dataset import Dataset
    from .server import EstimatorServer
    ctx = {"dsn": a.dsn}
    ests = _estimators(a.plugin or ["oracle"], ctx)
    if a.data:
        ds = Dataset(a.data)
        for e in ests:
            if not getattr(e, "pretrained", False):
                e.fit(ds)
                print(f"fitted {e.name} on {a.data}", file=sys.stderr)
    EstimatorServer(ests, a.socket, log_path=a.log, verbose=a.verbose,
                    guard=a.guard, guard_under=a.guard_under).serve_forever()


def cmd_collect(a):
    from .anonymize import Anonymizer
    from .collect import collect
    anon = Anonymizer(a.salt, a.anonymize) if a.anonymize else None
    counts = collect(a.dsn, a.out, label=a.label, database=a.database, anonymizer=anon)
    print(json.dumps(counts))


def cmd_anonymize(a):
    """Re-anonymize an existing (raw) dataset into a new directory."""
    import os
    import pyarrow as pa
    import pyarrow.parquet as pq
    from .anonymize import Anonymizer
    from .collect import read_manifest, write_manifest, write_part
    from .dataset import Dataset
    from .schema import plan_to_rows
    src = Dataset(a.data)
    if src.manifest.get("anonymization"):
        sys.exit("the source dataset is already anonymized")
    anon = Anonymizer(a.salt, a.level)
    os.makedirs(a.out, exist_ok=True)
    m = read_manifest(a.out)
    m.update({k: src.manifest.get(k) for k in ("last_seqid", "last_stats_at", "filter", "sources")})
    m["anonymization"] = anon.describe()
    plans = src.plans()
    nodes_by_seq: dict[int, list] = {}
    for n in src.nodes().to_pylist():
        nodes_by_seq.setdefault(n["seqid"], []).append(n)
    out_p, out_n, out_r = [], [], []
    for p in plans.to_pylist():
        meta = json.loads(p["meta"]) if p["meta"] else {}
        raw_nodes = []
        for n in nodes_by_seq.get(p["seqid"], []):
            d = {"id": n["node_id"], "parent": n["parent"], "role": n["role"], "type": n["type"],
                 "relids": n["relids"], "ident": n["ident"], "plan_rows": n["plan_rows"],
                 "startup_cost": n["startup_cost"], "total_cost": n["total_cost"], "width": n["width"],
                 "parallel_aware": n["parallel_aware"], "ntuples": n["ntuples"], "nloops": n["nloops"],
                 "done": n["done"], "censored": n["censored"], "censor_basis": n["censor_basis"],
                 "worker_ntuples": n["worker_ntuples"], "worker_loops": n["worker_loops"],
                 "nfiltered1": n["nfiltered1"], "nfiltered2": n["nfiltered2"],
                 "inner_unique_child": n["inner_unique_child"], "point": n["point"],
                 "param_outer": n["param_outer"]}
            for k in ("ce", "relinfo", "index"):
                if n[k]:
                    d[k] = json.loads(n[k])
            if n["est_source"] is not None or n["est_answer"] is not None:
                d["est"] = {"pg": n["est_pg"], "answer": n["est_answer"], "source": n["est_source"],
                            "applied": n["est_applied"], "route": n["est_route"]}
            raw_nodes.append(d)
        rec = dict(p, nodes=raw_nodes, meta=meta)
        rec = anon.plan_record(rec)
        pp, nn, rr = plan_to_rows(rec)
        out_p.append(pp)
        out_n.extend(nn)
        out_r.extend(rr)
    m["parts"] = 1
    write_part(a.out, "plans", out_p, 1)
    write_part(a.out, "nodes", out_n, 1)
    write_part(a.out, "requests", out_r, 1)
    srows = [anon.stats_record(r) for r in src.stats().to_pylist()]
    write_part(a.out, "stats", srows, 1)
    write_manifest(a.out, m)
    print(json.dumps({"plans": len(out_p), "nodes": len(out_n), "stats": len(srows)}))


def cmd_info(a):
    from .dataset import Dataset
    print(json.dumps(Dataset(a.data).summary(), indent=2, default=str))


def cmd_qerror(a):
    from .dataset import Dataset
    from .metrics import Split, evaluate, format_report
    from .metrics import ExampleView
    ds = Dataset(a.data)
    ests = _estimators(a.plugin or [], {"dsn": a.dsn})
    split = Split(a.test_fraction, a.split_by) if a.test_fraction else None
    fit_data = Dataset(a.fit_data) if a.fit_data else ds
    keep = (lambda ex: not split.is_test(ex)) if (split and not a.fit_data) else \
        ((lambda ex: ex.label in a.fit_label) if a.fit_label else (lambda ex: True))
    for e in ests:
        if not getattr(e, "pretrained", False):
            e.fit(ExampleView(fit_data, keep))
    res = evaluate(ds, ests, points=a.point, labels=a.label, split=split, group_by=a.group_by,
                   fit=False, propagate=not a.no_propagate)
    print(json.dumps(res, indent=2) if a.json else format_report(res))


def cmd_cefeedback(a):
    from .dataset import Dataset
    from .estimator import load
    est = load(f"ce-feedback:store={a.store},threshold={a.threshold},tolerance={a.tolerance}",
               {"dsn": a.dsn})
    est.fit(Dataset(a.data))
    for line in est.decisions or ["(no change)"]:
        print(line)
    if a.verbose:
        print("-- store")
        for line in est.describe():
            print(line)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python3 -m pgpi", description="pg_plan_inspector 2 framework")
    ap.add_argument("--version", action="version", version=f"pgpi {__version__}")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("serve", help="answer the planner's cardinality requests")
    p.add_argument("--socket", default="/tmp/pgpi.sock")
    p.add_argument("--dsn", default="")
    p.add_argument("--plugin", action="append", help="estimator spec")
    p.add_argument("--data", help="dataset to fit the estimators on before serving")
    p.add_argument("--log", help="append every request/answer as JSON lines")
    p.add_argument("--guard", type=float, metavar="K",
                   help="abstain when an answer is more than K times above or below PostgreSQL's estimate")
    p.add_argument("--guard-under", type=float, metavar="U",
                   help="abstain when an answer is more than U times below PostgreSQL's estimate "
                        "(overrides --guard below; alone, only answers below are checked)")
    p.add_argument("--verbose", action="store_true")
    p.set_defaults(func=cmd_serve)

    p = sub.add_parser("collect", help="append pgpi.plans/pgpi.stats to a Parquet dataset")
    p.add_argument("--dsn", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--label", help="SQL LIKE pattern on the label")
    p.add_argument("--database")
    p.add_argument("--anonymize", choices=["names", "full"])
    p.add_argument("--salt")
    p.set_defaults(func=cmd_collect)

    p = sub.add_parser("anonymize", help="write an anonymized copy of a dataset")
    p.add_argument("--data", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--level", choices=["names", "full"], default="full")
    p.add_argument("--salt", required=True)
    p.set_defaults(func=cmd_anonymize)

    p = sub.add_parser("info", help="summarize a dataset")
    p.add_argument("--data", required=True)
    p.set_defaults(func=cmd_info)

    p = sub.add_parser("qerror", help="q-error of PostgreSQL and estimators on observed nodes")
    p.add_argument("--data", required=True)
    p.add_argument("--plugin", action="append")
    p.add_argument("--dsn", default="")
    p.add_argument("--point", action="append")
    p.add_argument("--label", action="append")
    p.add_argument("--group-by", choices=["point", "rels"], default="point")
    p.add_argument("--test-fraction", type=float, help="fit on the rest, score on this fraction")
    p.add_argument("--split-by", choices=["queryid", "seqid", "label", "template"], default="queryid")
    p.add_argument("--fit-data", help="fit on this dataset instead")
    p.add_argument("--fit-label", action="append", help="fit only on examples with this label")
    p.add_argument("--no-propagate", action="store_true",
                   help="score each answer alone, not as the planner would combine them")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_qerror)


    p = sub.add_parser("cefeedback", help="one round of CE feedback (switch model assumptions per query)")
    p.add_argument("--data", required=True, help="dataset with the executions since the last round")
    p.add_argument("--store", required=True, help="hint store (JSON), created if missing")
    p.add_argument("--threshold", type=float, default=2.0, help="q-error that counts as a model error")
    p.add_argument("--tolerance", type=float, default=0.2, help="allowed execution-time regression")
    p.add_argument("--dsn", default="")
    p.add_argument("--verbose", action="store_true")
    p.set_defaults(func=cmd_cefeedback)

    a = ap.parse_args(argv)
    if getattr(a, "anonymize", None) and not getattr(a, "salt", None):
        ap.error("--anonymize needs --salt")
    a.func(a)


if __name__ == "__main__":
    main()
