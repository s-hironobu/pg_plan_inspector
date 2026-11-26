"""The benchmark runner: every estimator by execution time on one workload.

Used by benchmark/stats_ceb, job_light, ceb_1a and by demo/run.py.

1. Prepare the data (the prepare step of the benchmark).
2. Capture train.sql with PostgreSQL alone, and collect it into a dataset.
3. Fit every estimator on the dataset, and start one estimation server per estimator.
4. Run test.sql with each estimator (enforce) and with PostgreSQL alone.
   The queries run one by one, with the estimators in a shuffled order, --reps times.
   Settings: parallel query off, random_page_cost = 1.1, statement_timeout = --timeout.
5. Report, for A and B separately:
   - the sum of the median execution times of the queries (server side,
     pgpi.plans.execution_ms). A canceled query counts as --timeout. So does a query
     that failed, e.g. by exceeding --temp-limit of temporary files.
   - the number of queries whose plan differs from the plan of PostgreSQL
   - the number of timeouts
   - the q-error of the estimates that the planner used
   per_query.tsv has every query.
"""

from __future__ import annotations

import argparse
import collections
import contextlib
import importlib.util
import os
import random
import shutil
import statistics
import time
from dataclasses import dataclass, field
from typing import Callable

import psycopg

import pgpi
from pgpi.collect import collect
from pgpi.schema import q_error
from pgpi.server import EstimatorServer

from . import DEFAULT_DSN, out_dir
from .runner import reset_log, sock_path

PLUGINS = ["feedback", "matcomp", "hres", "litecard", "nngp", "grasp"]
QUICK = PLUGINS
# the Python packages that an estimator needs in addition to pgpi (the estimator is skipped without them)
REQUIRES = {"grasp": ("torch",), "litecard": ("sqlglot", "sklearn")}
# settings that keep the training time reasonable on a 2-core machine (the fit uses about 5000 nodes)
SPEC_EXTRA = {"grasp": "epochs=100,restarts=1"}
QUICK_EXTRA = {"grasp": "epochs=30,restarts=1", "matcomp": "restarts=1"}


@dataclass
class Bench:
    name: str                               # the benchmark, e.g. "stats_ceb" (default output: benchmark/data/<name>)
    here: str                               # its path (train.sql, test.sql)
    prefix: str                             # of the labels: <prefix>-train, <prefix>-postgres, <prefix>-<plugin>
    data_title: str                         # heading of step 1
    prepare: Callable                       # (conn, args, say): the data is there afterwards
    search_path: str
    timeout: int = 30                       # default --timeout, seconds
    train: tuple | None = None              # (default, quick) number of training queries; None: all
    extra_args: list = field(default_factory=list)   # [(args, kwargs)] for argparse
    workload: Callable | None = None        # (args, say) -> (train.sql, test.sql) paths; default: in here
    out: str | None = None                  # default --out; None: benchmark/data/<name>


def read_workload(path):
    """Return [(group, name, sql)]. A '-- A q12' line gives the name of the query on the next line."""
    out, group, name = [], None, None
    for line in open(path):
        line = line.strip()
        if line.startswith("-- ") and len(line.split()) == 3 and line.split()[1] in ("A", "B", "train"):
            _, group, name = line.split()
        elif line.startswith("SELECT"):
            out.append((group, name, line.rstrip(";")))
    return out


def missing(plugin: str) -> list[str]:
    """Return the packages that an estimator needs and that are not installed."""
    return [m for m in REQUIRES.get(plugin.split(":")[0], ()) if importlib.util.find_spec(m) is None]


def main(b: Bench, doc: str | None = None):
    ap = argparse.ArgumentParser(description=doc, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--out", default=b.out or out_dir(b.name))
    for args, kw in b.extra_args:
        ap.add_argument(*args, **kw)
    ap.add_argument("--plugins", help="comma-separated plugins, each a spec such as grasp:epochs=50 "
                                      "(default: all bundled that can run here)")
    if b.train:
        ap.add_argument("--train", type=int, help=f"training queries, the first N of train.sql "
                                                  f"(default {b.train[0]}, {b.train[1]} with --quick)")
    ap.add_argument("--reps", type=int, default=3, help="runs of each query per estimator (the median is used)")
    ap.add_argument("--timeout", type=int, default=b.timeout, help="statement_timeout in seconds")
    ap.add_argument("--temp-limit", default="2GB",
                    help="temp_file_limit of the session (a query over it fails and counts as a timeout); "
                         "'off' leaves the server's setting (default 2GB)")
    ap.add_argument("--guard", type=float, metavar="K",
                    help="as serve --guard: abstain on answers more than K times off PostgreSQL's estimate")
    ap.add_argument("--guard-under", type=float, metavar="U",
                    help="as serve --guard-under: abstain on answers more than U times below PostgreSQL's estimate")
    ap.add_argument("--parallel", type=int, default=0, metavar="N",
                    help="max_parallel_workers_per_gather for every run, training and test (default 0: no parallel query)")
    ap.add_argument("--quick", action="store_true", help="1 run, shorter training")
    ap.add_argument("--report-only", action="store_true",
                    help="only collect and report the runs already in pgpi.plans (after a failed report)")
    a = ap.parse_args()
    if not a.report_only:
        shutil.rmtree(a.out, ignore_errors=True)
    os.makedirs(a.out, exist_ok=True)
    lines = []

    def say(*xs):
        s = " ".join(str(x) for x in xs)
        print(s, flush=True)
        lines.append(s)
    say.lines = lines

    if a.plugins:
        plugins = a.plugins.split(",")
    else:
        plugins = QUICK if a.quick else PLUGINS
    skipped = [f"{m} (no {', '.join(missing(m))})" for m in plugins if missing(m)]
    plugins = [m for m in plugins if not missing(m)]
    reps = 1 if a.quick else a.reps
    train_sql, test_sql = (b.workload(a, print) if b.workload
                           else (os.path.join(b.here, "train.sql"), os.path.join(b.here, "test.sql")))
    train = read_workload(train_sql)
    if b.train:
        train = train[:a.train or (b.train[1] if a.quick else b.train[0])]
    test = read_workload(test_sql)
    p = b.prefix

    # prepare_threshold=None: otherwise psycopg turns a query that runs 5 times into a
    # prepared statement. PostgreSQL then reuses the cached plan without planning again.
    # A run with one estimator (or PostgreSQL alone) would execute a plan made under another.
    conn = psycopg.connect(a.dsn, autocommit=True, prepare_threshold=None)
    ds_dir = os.path.join(a.out, "ds")
    if a.report_only:
        found = [r[0] for r in conn.execute(f"SELECT DISTINCT label FROM pgpi.plans WHERE label LIKE '{p}-%%' "
                                            f"AND label <> '{p}-train' ORDER BY 1").fetchall()]
        labels = {lab[len(p) + 1:]: lab for lab in found}
        conn.execute("SELECT pgpi_log_flush(60000)")
        collect(a.dsn, ds_dir)
        report(a, b, conn, test, labels, ds_dir, say)
        return
    say(f"== 1. {b.data_title}")
    b.prepare(conn, a, say)
    reset_log(conn)
    conn.execute(f"SET search_path TO {b.search_path}")
    conn.execute("SET random_page_cost TO 1.1")
    conn.execute(f"SET max_parallel_workers_per_gather TO {int(a.parallel)}")
    conn.execute(f"SET statement_timeout TO '{a.timeout}s'")
    conn.execute("SET pgpi.callback_timeout TO 600000")
    if a.temp_limit != "off":
        try:
            conn.execute(f"SET temp_file_limit TO '{a.temp_limit}'")
        except psycopg.errors.InsufficientPrivilege:
            say(f"   (temp_file_limit not set: superuser only; a query that fills the disk stops the run)")

    say(f"== 2. capture train.sql ({len(train)} queries) with PostgreSQL alone, collect")
    t0, canceled, failed = time.time(), 0, 0
    conn.execute("SET pgpi.log_plans TO on")
    conn.execute(f"SET pgpi.label TO '{p}-train'")
    for i, (_, _, q) in enumerate(train, 1):
        try:
            conn.execute(q).fetchall()
        except psycopg.errors.QueryCanceled:
            canceled += 1                       # captured as canceled: the nodes that finished still teach
        except (psycopg.errors.ConfigurationLimitExceeded, psycopg.errors.DiskFull):
            failed += 1                         # over temp_file_limit (or disk full): not captured
        if i % 100 == 0:
            print(f"   {i}/{len(train)} ({time.time() - t0:.0f}s)", flush=True)
    conn.execute("SET pgpi.log_plans TO off")
    conn.execute("SELECT pgpi_log_flush(60000)")
    c = collect(a.dsn, ds_dir)
    say(f"   {time.time() - t0:.0f}s" + (f", {canceled} canceled at {a.timeout}s" if canceled else "")
        + (f", {failed} over temp_file_limit {a.temp_limit}" if failed else "") + f"; dataset: {c}")

    say(f"== 3. fit {', '.join(plugins)}" + (f" (skipped: {', '.join(skipped)})" if skipped else ""))
    ds = pgpi.Dataset(ds_dir)
    ests = {}
    for m in plugins:
        spec = m
        extra = (QUICK_EXTRA if a.quick else SPEC_EXTRA).get(m.split(":")[0])
        if extra and ":" not in m:                      # an explicit spec in --plugins is used as given
            spec += ":" + extra
        print(f"   {spec} ...", end="", flush=True)
        t0 = time.time()
        e = pgpi.load(spec, {"dsn": a.dsn})
        e.fit(ds)
        ests[m] = e
        print("\r", end="")
        say(f"   {spec:<34} {time.time() - t0:6.1f}s")

    labels = {"postgres": f"{p}-postgres", **{m: f"{p}-{m.split(':')[0]}" for m in plugins}}
    say(f"== 4. run test.sql ({len(test)} queries: {sum(g == 'A' for g, _, _ in test)} A, "
        f"{sum(g == 'B' for g, _, _ in test)} B) x {len(labels)} x {reps} run(s), timeout {a.timeout}s"
        + (f", guard {a.guard:g}" if a.guard else "") + (f", guard-under {a.guard_under:g}" if a.guard_under else "")
        + (f", parallel {a.parallel}" if a.parallel else ""))
    rng = random.Random(0)
    t0 = time.time()
    failures = open(os.path.join(a.out, "failures.tsv"), "w")   # failed runs leave no row in pgpi.plans
    with contextlib.ExitStack() as stack:
        socks = {}
        for m, e in ests.items():
            socks[m] = sock_path(f"{p}_{len(socks)}")
            stack.enter_context(EstimatorServer([e], socks[m], guard=a.guard, guard_under=a.guard_under))
        conn.execute("SET pgpi.log_plans TO on")
        for rep in range(reps):
            for _, _, q in test:
                order = list(labels)
                rng.shuffle(order)
                for m in order:
                    if m == "postgres":
                        conn.execute("SET pgpi.mode TO off")
                    else:
                        conn.execute("SET pgpi.mode TO enforce")
                        conn.execute(f"SET pgpi.callback TO '{socks[m]}'")
                    conn.execute(f"SET pgpi.label TO '{labels[m]}'")
                    try:
                        conn.execute(q).fetchall()
                    except psycopg.errors.QueryCanceled:
                        pass                    # captured with status 'canceled'; counted in the report
                    except (psycopg.errors.ConfigurationLimitExceeded, psycopg.errors.DiskFull) as e:
                        failures.write(f"{labels[m]}\t{q}\t{str(e).strip()}\n")
                        failures.flush()
            say(f"   run {rep + 1}/{reps} done ({time.time() - t0:.0f}s)")
        conn.execute("SET pgpi.log_plans TO off")
        conn.execute("RESET pgpi.mode")
    failures.close()
    conn.execute("SELECT pgpi_log_flush(60000)")
    collect(a.dsn, ds_dir)
    report(a, b, conn, test, labels, ds_dir, say)


def report(a, b, conn, test, labels, ds_dir, say):
    """Step 5, from pgpi.plans (times, plans, timeouts) and the dataset (q-error)."""
    group_of = {sql: g for g, _, sql in test}
    ds = pgpi.Dataset(ds_dir)
    say("== 5. results")
    rows = conn.execute(f"SELECT label, query, execution_ms, planid, status, (meta::jsonb)->>'mode' FROM pgpi.plans "
                        f"WHERE label LIKE '{b.prefix}-%%' AND label <> '{b.prefix}-train'").fetchall()
    ms = collections.defaultdict(list)          # (plugin, sql) -> [ms]
    plan = collections.defaultdict(collections.Counter)
    by_label = {v: k for k, v in labels.items()}
    timeouts = collections.Counter()
    reused = collections.Counter()              # runs of a plan made in the other mode: a plan not made for them
    for label, q, t, planid, status, planned in rows:
        q = q.rstrip(";")
        if q not in group_of or label not in by_label:
            continue
        m = by_label[label]
        if planned and planned != ("off" if m == "postgres" else "enforce"):
            reused[m] += 1
        ms[(m, q)].append(a.timeout * 1000.0 if status == "canceled" else (t or 0.0))
        plan[(m, q)][planid] += 1
        if status == "canceled":
            timeouts[m] += 1
    fpath = os.path.join(a.out, "failures.tsv")
    failed = collections.Counter()
    for line in (open(fpath) if os.path.exists(fpath) else []):
        label, q = line.rstrip("\n").split("\t")[:2]
        if q in group_of and label in by_label:
            m = by_label[label]
            ms[(m, q)].append(a.timeout * 1000.0)
            timeouts[m] += 1
            failed[m] += 1
    # A query canceled while it was still being planned leaves no row. Count it as a
    # timeout, not as 0 ms. The expected runs are the most that any (estimator, query) has.
    expected = max((len(v) for v in ms.values()), default=0)
    unplanned = collections.Counter()
    for m in labels:
        for _, _, q in test:
            n = expected - len(ms[(m, q)])
            if n > 0:
                ms[(m, q)].extend([a.timeout * 1000.0] * n)
                timeouts[m] += n
                unplanned[m] += n
    med = {k: statistics.median(v) for k, v in ms.items()}
    main_plan = {k: c.most_common(1)[0][0] for k, c in plan.items()}

    nodes = ds.nodes(["label", "usable", "est_plan", "true_rows"]).to_pylist()
    qerr = collections.defaultdict(list)
    for n in nodes:
        if n["usable"] and n["label"] in by_label:
            e = q_error(n["est_plan"], n["true_rows"])
            if e is not None:
                qerr[by_label[n["label"]]].append(e)

    reps = max((len(v) for v in ms.values()), default=0)
    say(f"   execution time: sum over the queries of the median of {reps} run(s), ms; "
        f"'changed' = queries whose plan differs from PostgreSQL's; "
        f"'timeouts' = runs canceled at {a.timeout}s or failed (counted as {a.timeout}s)")
    say(f"   {'estimator':<12} {'A ms':>9} {'vs pg':>6} {'B ms':>9} {'vs pg':>6} {'all ms':>9} {'vs pg':>6} "
        f"{'changed':>8} {'timeouts':>8} {'q-error median':>15} {'p90':>8}")
    tot = {}
    for m in labels:
        tot[m] = {g: sum(med.get((m, q), 0.0) for gg, _, q in test if gg == g) for g in ("A", "B")}
        tot[m]["all"] = tot[m]["A"] + tot[m]["B"]
    for m in sorted(labels, key=lambda m: tot[m]["all"]):
        changed = sum(1 for _, _, q in test if (m, q) in main_plan and ("postgres", q) in main_plan
                      and main_plan[(m, q)] != main_plan[("postgres", q)])
        qs = sorted(qerr[m])
        qmed = statistics.median(qs) if qs else float("nan")
        p90 = qs[int(0.9 * (len(qs) - 1))] if qs else float("nan")
        cells = []
        for g in ("A", "B", "all"):
            cells.append(f"{tot[m][g]:9.0f} {tot[m][g] / max(tot['postgres'][g], 1e-9):6.2f}")
        say(f"   {m:<12} " + " ".join(cells) + f" {changed:>8} {timeouts[m]:>8} {qmed:>15.2f} {p90:>8.1f}")
    if reused:
        say("   WARNING: runs that executed a plan made in another mode (a reused cached plan; the results are not "
            "valid): " + ", ".join(f"{m} {n}" for m, n in sorted(reused.items())))
    if unplanned:
        say("   canceled while planning (no plan captured; in timeouts): "
            + ", ".join(f"{m} {n}" for m, n in sorted(unplanned.items())))
    if failed:
        say(f"   failed (over temp_file_limit, in timeouts): "
            + ", ".join(f"{m} {n}" for m, n in sorted(failed.items())) + f"; see {fpath}")

    with open(os.path.join(a.out, "per_query.tsv"), "w") as f:
        f.write("group\tquery\t" + "\t".join(labels) + "\n")
        for g, n, q in test:
            f.write(f"{g}\t{n}\t" + "\t".join(f"{med.get((m, q), float('nan')):.1f}" for m in labels) + "\n")
    say(f"   per query: {os.path.join(a.out, 'per_query.tsv')}")
    with open(os.path.join(a.out, "report.txt"), "w") as f:
        f.write("\n".join(say.lines) + "\n")
