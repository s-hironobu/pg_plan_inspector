#!/usr/bin/env python3
"""Continual learning on CEB-IMDb 1a (experimental).

Question: do estimators improve and stay stable when they keep learning from the
plans that they lead to themselves?

Cycle 0: the initial training queries run with PostgreSQL alone. Every plugin is
fitted on them.

Then, in every cycle k = 1..K, four sets of queries run with every plugin (and with
PostgreSQL alone):

  R    repeat      the same queries in every cycle. Learned from.
                   (the production view: recurring queries)
  N    new         new queries in every cycle. Learned from.
                   (first sight, and more observations)
  Es   eval seen   the same queries in every cycle, of the information kinds that
                   were trained on. Never learned from.
                   (the research view: generalization)
  Eu   eval unseen the same queries in every cycle, of other information kinds.
                   Never learned from.

After the runs, each plugin is refitted on everything that it has learned from so
far. These are the initial training plans, and the R and N plans of its own runs.
Each plugin learns from the plans that it led to, not from the plans of the others.

With --frozen (default), each plugin also runs in its cycle-0 version, which is
never refitted. This is a control. In cycle 1 it is the same as the live plugin, so
it runs from cycle 2.

    python3 benchmark/ceb_1a_continual/run.py --dsn "$DSN"
    python3 benchmark/ceb_1a_continual/run.py --dsn "$DSN" --cycles 3 --no-frozen

The queries are those of benchmark/ceb_1a/queries.txt (downloaded once, as for
ceb_1a). The initial queries, R and N come from its training queries. Es comes from
its A queries, and Eu from its B queries. All sets are disjoint. The IMDb data must be
prepared as for ceb_1a (benchmark/README.md).

Output (--out, default benchmark/data/ceb_1a_continual; emptied first):
  cycle0/             the initial training runs (queries.txt, and ds/ once they are collected)
  cycle<k>/           cycle k. It is created when the cycle starts, with queries.txt
                      (its queries, by set). ds/ has the runs of all runners and sets
                      once the cycle is done.
  train/ds/           everything, accumulated (what the plugins are fitted on)
  report.txt          the results per cycle and set, and the trends over the cycles
  per_query.tsv       every query, cycle and runner
  output.txt          what was printed
Labels: cc-c0-init and cc-c<k>-<runner>-<set>. <runner> is postgres, a plugin, or
<plugin>-frozen.
"""

from __future__ import annotations

import argparse
import collections
import contextlib
import os
import random
import shutil
import statistics
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import lib  # noqa: E402,F401  (makes client/ importable without PYTHONPATH)
from lib import DEFAULT_DSN, ceb, imdb, out_dir  # noqa: E402
from lib.bench import missing  # noqa: E402
from lib.runner import reset_log, sock_path  # noqa: E402

import psycopg  # noqa: E402

import pgpi  # noqa: E402
from pgpi.collect import collect  # noqa: E402
from pgpi.metrics import ExampleView  # noqa: E402
from pgpi.schema import q_error  # noqa: E402
from pgpi.server import EstimatorServer  # noqa: E402

PLUGINS = ["feedback", "litecard", "nngp", "hres"]     # the ones that train in seconds
SETS = ("R", "N", "Es", "Eu")
LEARNED = ("R", "N")
P = "cc"
# a query counts as slower than in the previous cycle when both of these hold
SLOWER_FACTOR, SLOWER_MS = 1.5, 100.0


def pick(a):
    """Return the query names as {'init': [...], 'R': [...], 'N': [[...] per cycle], 'Es': [...], 'Eu': [...]}."""
    rows = [line.split() for line in open(os.path.join(HERE, "..", "ceb_1a", "queries.txt"))
            if line.strip() and not line.startswith("#")]
    train = [r[1] for r in rows if r[0] == "train"]
    A = [r[1] for r in rows if r[0] == "A"]
    B = [r[1] for r in rows if r[0] == "B"]
    need = a.init + a.repeat + a.new * a.cycles
    if need > len(train) or a.eval > len(A) or a.eval > len(B):
        sys.exit(f"not enough queries: {need} of {len(train)} training, {a.eval} of {len(A)} A / {len(B)} B")
    i = a.init + a.repeat
    return {"init": train[:a.init], "R": train[a.init:i],
            "N": [train[i + a.new * k:i + a.new * (k + 1)] for k in range(a.cycles)],
            "Es": A[:a.eval], "Eu": B[:a.eval]}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--out", default=out_dir("ceb_1a_continual"))
    ap.add_argument("--plugins", default=",".join(PLUGINS), help=f"comma-separated plugins (default {','.join(PLUGINS)})")
    ap.add_argument("--cycles", type=int, default=5, help="learning cycles after cycle 0 (default 5)")
    ap.add_argument("--init", type=int, default=50, help="initial training queries, cycle 0 (default 50)")
    ap.add_argument("--repeat", type=int, default=20, help="R: queries repeated in every cycle (default 20)")
    ap.add_argument("--new", type=int, default=20, help="N: new queries per cycle (default 20)")
    ap.add_argument("--eval", type=int, default=15, help="Es and Eu: queries each (default 15)")
    ap.add_argument("--frozen", action=argparse.BooleanOptionalAction, default=True,
                    help="also run each plugin's cycle-0 version as a control (default on)")
    ap.add_argument("--reps", type=int, default=1, help="runs of each query per cycle, the median is used (default 1)")
    ap.add_argument("--timeout", type=int, default=60, help="statement_timeout in seconds (default 60)")
    ap.add_argument("--temp-limit", default="3GB", help="temp_file_limit, 'off': the server's (default 3GB)")
    ap.add_argument("--parallel", type=int, default=0, metavar="N", help="max_parallel_workers_per_gather (default 0)")
    ap.add_argument("--guard-under", type=float, metavar="U", help="as serve --guard-under")
    a = ap.parse_args()

    shutil.rmtree(a.out, ignore_errors=True)
    os.makedirs(a.out)
    lines = []

    def say(*xs):
        s = " ".join(str(x) for x in xs)
        print(s, flush=True)
        lines.append(s)

    plugins = [m for m in a.plugins.split(",") if m]
    skipped = [f"{m} (no {', '.join(missing(m))})" for m in plugins if missing(m)]
    plugins = [m for m in plugins if not missing(m)]
    names = pick(a)
    allnames = names["init"] + names["R"] + [q for n in names["N"] for q in n] + names["Es"] + names["Eu"]
    sql = ceb.fetch(allnames, say)
    q = {k: [sql[n] for n in v] for k, v in names.items() if k != "N"}
    q["N"] = [[sql[n] for n in v] for v in names["N"]]
    qname = {sql[n]: n for n in allnames}

    # prepare_threshold=None: otherwise psycopg turns a query that runs 5 times into a
    # prepared statement. PostgreSQL then reuses the cached plan without planning again.
    # A run with one estimator (or PostgreSQL alone) would execute a plan made under another.
    conn = psycopg.connect(a.dsn, autocommit=True, prepare_threshold=None)
    say("== IMDb data (schema imdb)")
    try:
        imdb.require(conn, tables=imdb.CEB_1A)
    except imdb.NotLoaded as e:
        sys.exit(f"   {e}")
    reset_log(conn)
    conn.execute("SET search_path TO imdb, public")
    conn.execute("SET random_page_cost TO 1.1")
    conn.execute(f"SET max_parallel_workers_per_gather TO {int(a.parallel)}")
    conn.execute(f"SET statement_timeout TO '{a.timeout}s'")
    conn.execute("SET pgpi.callback_timeout TO 600000")
    if a.temp_limit != "off":
        try:
            conn.execute(f"SET temp_file_limit TO '{a.temp_limit}'")
        except psycopg.errors.InsufficientPrivilege:
            say("   (temp_file_limit not set: superuser only)")
    train_ds = os.path.join(a.out, "train", "ds")

    def run(queries, label, sock=None):
        """Run queries under one label. Without sock, PostgreSQL runs alone."""
        if sock is None:
            conn.execute("SET pgpi.mode TO off")
        else:
            conn.execute("SET pgpi.mode TO enforce")
            conn.execute(f"SET pgpi.callback TO '{sock}'")
        conn.execute(f"SET pgpi.label TO '{label}'")
        for s in queries:
            try:
                conn.execute(s).fetchall()
            except psycopg.errors.QueryCanceled:
                pass                                # captured as canceled, or not at all if still planning
            except (psycopg.errors.ConfigurationLimitExceeded, psycopg.errors.DiskFull):
                pass                                # not captured: counted as a timeout in the report
        conn.execute("RESET pgpi.mode")

    def cycle_dir(k, sets):
        """Create cycle<k>/ when the cycle starts, with the names of its queries."""
        d = os.path.join(a.out, f"cycle{k}")
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "queries.txt"), "w") as f:
            for s, qs in sets.items():
                for one in qs:
                    f.write(f"{s}\t{qname.get(one, '?')}\n")
        return os.path.relpath(d, os.path.dirname(a.out))

    def flush_collect(k):
        conn.execute("SELECT pgpi_log_flush(60000)")
        c = collect(a.dsn, os.path.join(a.out, f"cycle{k}", "ds"), label=f"{P}-c{k}-%")
        collect(a.dsn, train_ds)
        return c

    def fit(m, k):
        """Return plugin m fitted on the initial plans and its own R and N plans of cycles 1..k."""
        keep = {f"{P}-c0-init"} | {f"{P}-c{j}-{m}-{s}" for j in range(1, k + 1) for s in LEARNED}
        e = pgpi.load(m, {"dsn": a.dsn})
        e.fit(ExampleView(pgpi.Dataset(train_ds), lambda ex: ex.label in keep))
        return e

    # ------------------------------------------------------------ cycle 0
    d = cycle_dir(0, {"init": q["init"]})
    say(f"== cycle 0 of {a.cycles}: {len(q['init'])} initial training queries with PostgreSQL alone -> {d}/"
        + (f" (skipped: {', '.join(skipped)})" if skipped else ""))
    t0 = time.time()
    conn.execute("SET pgpi.log_plans TO on")
    run(q["init"], f"{P}-c0-init")
    c = flush_collect(0)
    say(f"   {time.time() - t0:.0f}s; {d}/ds: {c}")
    live, frozen = {}, {}
    for m in plugins:
        t1 = time.time()
        live[m] = fit(m, 0)
        say(f"   fit {m:<12} {time.time() - t1:6.1f}s")
    if a.frozen:
        frozen = dict(live)

    # ------------------------------------------------------------ cycles 1..K
    rng = random.Random(0)
    for k in range(1, a.cycles + 1):
        runners = ["postgres"] + plugins + ([f"{m}-frozen" for m in plugins] if a.frozen and k >= 2 else [])
        sets = {"R": q["R"], "N": q["N"][k - 1], "Es": q["Es"], "Eu": q["Eu"]}
        nq = sum(len(v) for v in sets.values())
        d = cycle_dir(k, sets)
        say(f"== cycle {k} of {a.cycles}: {nq} queries (R {len(sets['R'])}, N {len(sets['N'])}, Es {len(sets['Es'])}, "
            f"Eu {len(sets['Eu'])}) x {len(runners)} x {a.reps} run(s) -> {d}/")
        t0 = time.time()
        with contextlib.ExitStack() as stack:
            socks = {}
            for r in runners:
                if r == "postgres":
                    continue
                e = frozen[r[:-len("-frozen")]] if r.endswith("-frozen") else live[r]
                socks[r] = sock_path(f"cc{k}_{len(socks)}")
                stack.enter_context(EstimatorServer([e], socks[r], guard_under=a.guard_under))
            conn.execute("SET pgpi.log_plans TO on")
            for _ in range(a.reps):
                for s, qs in sets.items():
                    for one in qs:
                        order = list(runners)
                        rng.shuffle(order)
                        for r in order:
                            run([one], f"{P}-c{k}-{r}-{s}", socks.get(r))
                    print(f"   {s} done ({time.time() - t0:.0f}s)", flush=True)
            conn.execute("SET pgpi.log_plans TO off")
        c = flush_collect(k)
        say(f"   ran in {time.time() - t0:.0f}s; {d}/ds: {c}")
        if k < a.cycles:
            for m in plugins:
                t1 = time.time()
                live[m] = fit(m, k)
                say(f"   refit {m:<12} {time.time() - t1:6.1f}s")

    start = len(lines)
    report(a, conn, plugins, q, qname, train_ds, say)
    with open(os.path.join(a.out, "report.txt"), "w") as f:
        f.write("\n".join(lines[start:]) + "\n")
    with open(os.path.join(a.out, "output.txt"), "w") as f:
        f.write("\n".join(lines) + "\n")


def report(a, conn, plugins, q, qname, train_ds, say):
    say("== results")
    runners = ["postgres"] + plugins + ([f"{m}-frozen" for m in plugins] if a.frozen else [])
    sets_of = {k: {"R": q["R"], "N": q["N"][k - 1], "Es": q["Es"], "Eu": q["Eu"]} for k in range(1, a.cycles + 1)}
    rows = conn.execute(f"SELECT label, query, execution_ms, planid, status, (meta::jsonb)->>'mode' FROM pgpi.plans "
                        f"WHERE label LIKE '{P}-c%%' AND label <> '{P}-c0-init'").fetchall()
    ms = collections.defaultdict(list)          # (k, runner, set, sql) -> [ms]
    plan = {}
    reused = collections.Counter()              # runs of a plan made in the other mode (a reused cached plan)
    for label, sql, t, planid, status, planned in rows:
        parts = label.split("-")                # cc, c<k>, runner (may contain '-'), set
        k, s, r = int(parts[1][1:]), parts[-1], "-".join(parts[2:-1])
        if planned and planned != ("off" if r == "postgres" else "enforce"):
            reused[r] += 1
        sql = sql.rstrip(";")
        ms[(k, r, s, sql)].append(a.timeout * 1000.0 if status == "canceled" else (t or 0.0))
        plan[(k, r, s, sql)] = planid
    to = a.timeout * 1000.0

    def med(k, r, s, sql):
        v = ms.get((k, r, s, sql))
        return statistics.median(v) if v else to         # no row: canceled while planning, or failed

    ran = {(k, r) for (k, r, _, _) in ms}
    ds = pgpi.Dataset(train_ds)
    qerr = collections.defaultdict(list)
    for n in ds.nodes(["label", "usable", "est_plan", "true_rows"]).to_pylist():
        lab = n["label"] or ""
        if n["usable"] and lab.startswith(f"{P}-c") and lab != f"{P}-c0-init":
            parts = lab.split("-")
            e = q_error(n["est_plan"], n["true_rows"])
            if e is not None:
                qerr[(int(parts[1][1:]), "-".join(parts[2:-1]), parts[-1])].append(e)

    if reused:
        say("   WARNING: runs that executed a plan made in another mode (a reused cached plan; the results are not "
            "valid): " + ", ".join(f"{r} {n}" for r, n in sorted(reused.items())))
    out = []
    trend = collections.defaultdict(dict)
    for k in range(1, a.cycles + 1):
        say(f"\n   cycle {k}: execution time vs PostgreSQL in the same cycle (sum of {a.reps}-run medians); "
            f"changed = plans differing from PostgreSQL's; slower = queries more than {SLOWER_FACTOR:g}x and "
            f"{SLOWER_MS:g} ms slower than in cycle {k - 1} (R, Es, Eu); flips = plan differs from cycle {k - 1}")
        say(f"   {'runner':<17}" + "".join(f" {s + ' vs pg':>9}" for s in SETS)
            + f" {'all':>7} {'changed':>8} {'timeouts':>8} {'slower':>7} {'flips':>6} {'q-err R':>8} {'q-err E':>8}")
        for r in runners:
            if (k, r) not in ran:
                continue
            cells, tot, tot_pg = [], 0.0, 0.0
            changed = timeouts = slower = flips = 0
            for s in SETS:
                qs = sets_of[k][s]
                t = sum(med(k, r, s, x) for x in qs)
                tp = sum(med(k, "postgres", s, x) for x in qs)
                cells.append(t / tp if tp else float("nan"))
                tot, tot_pg = tot + t, tot_pg + tp
                trend[(r, s)][k] = cells[-1]
                for x in qs:
                    m = med(k, r, s, x)
                    timeouts += m >= to
                    if r != "postgres" and plan.get((k, r, s, x)) != plan.get((k, "postgres", s, x)):
                        changed += 1
                    if s != "N" and (k - 1, r) in ran:
                        prev = med(k - 1, r, s, x)
                        slower += m > SLOWER_FACTOR * prev and m - prev > SLOWER_MS
                        flips += plan.get((k, r, s, x)) != plan.get((k - 1, r, s, x))
                    out.append((k, r, s, qname.get(x, "?"), m, med(k, "postgres", s, x),
                                plan.get((k, r, s, x)) or ""))
            trend[(r, "all")][k] = tot / tot_pg if tot_pg else float("nan")
            qr = qerr[(k, r, "R")]
            qe = qerr[(k, r, "Es")] + qerr[(k, r, "Eu")]
            say(f"   {r:<17}" + "".join(f" {c:9.2f}" for c in cells) + f" {tot / tot_pg:7.2f} {changed:>8} "
                f"{timeouts:>8} {slower if (k - 1, r) in ran else '-':>7} {flips if (k - 1, r) in ran else '-':>6} "
                f"{statistics.median(qr) if qr else float('nan'):8.2f} {statistics.median(qe) if qe else float('nan'):8.2f}")

    say("\n   trends: vs PostgreSQL per cycle (- : not run)")
    for s in SETS + ("all",):
        say(f"   {s:<4} {'runner':<17}" + "".join(f" {'c' + str(k):>6}" for k in range(1, a.cycles + 1)))
        for r in runners:
            if any(k in trend[(r, s)] for k in range(1, a.cycles + 1)) and r != "postgres":
                say(f"        {r:<17}" + "".join(f" {trend[(r, s)][k]:6.2f}" if k in trend[(r, s)] else f" {'-':>6}"
                                         for k in range(1, a.cycles + 1)))
    with open(os.path.join(a.out, "per_query.tsv"), "w") as f:
        f.write("cycle\trunner\tset\tquery\tms\tpostgres_ms\tplanid\n")
        for row in out:
            f.write("\t".join(str(x) for x in row) + "\n")


if __name__ == "__main__":
    main()
