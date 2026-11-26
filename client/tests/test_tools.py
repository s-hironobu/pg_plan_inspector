"""client/tools/qerror_runs.py on a small dataset (no database)."""

import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(__file__))

import test_core as T  # noqa: E402

TOOL = os.path.join(os.path.dirname(__file__), "..", "tools", "qerror_runs.py")


def dataset(tmp_path, labels):
    recs = []
    for i, label in enumerate(labels, 1):
        r = T._plan_record()
        r["seqid"], r["queryid"], r["label"] = i, i, label
        recs.append(r)
    T._write_dataset(str(tmp_path), recs)
    return str(tmp_path)


def run(*args):
    out = subprocess.run([sys.executable, TOOL, *args], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    return out.stdout


def test_by_label_and_point(tmp_path):
    d = dataset(tmp_path, ["ceb-train", "ceb-postgres", "ceb-hres", "other"])
    out = run("--data", d, "--like", "ceb-%", "--by-point")
    lines = [line.split() for line in out.splitlines()[1:]]
    assert {x[0] for x in lines} == {"ceb-train", "ceb-postgres", "ceb-hres"}       # 'other' left out
    assert {x[1] for x in lines} >= {"all", "base", "join2"}
    alls = [x for x in lines if x[1] == "all"]
    assert all(int(x[2]) == 3 for x in alls)                                       # three nodes per plan


def test_continual(tmp_path):
    labels = [f"cc-c{k}-{r}-{s}" for k in (1, 2) for r in ("postgres", "hres") for s in ("R", "Eu")]
    labels += ["cc-c2-hres-frozen-R", "cc-c0-init"]
    out = run("--data", dataset(tmp_path, labels), "--continual")
    rows = {tuple(line.split()[:2]): line for line in out.splitlines() if line.startswith(("postgres", "hres"))}
    assert set(rows) == {("postgres", "R"), ("postgres", "Eu"), ("hres", "R"), ("hres", "Eu"), ("hres-frozen", "R")}
    assert " - " in rows[("hres-frozen", "R")].split("R", 1)[1][:20]                # not run in cycle 1


SLOW = os.path.join(os.path.dirname(__file__), "..", "tools", "slow_queries.py")


def test_slow_queries(tmp_path):
    recs = []
    for i, (label, q, ms) in enumerate([("x-postgres", "select 1", 10.0), ("x-grasp", "select 1", 900.0),
                                        ("x-postgres", "select 2", 50.0), ("x-grasp", "select 2", 40.0)], 1):
        r = T._plan_record()
        r.update(seqid=i, queryid=i, label=label, query=q, execution_ms=ms)
        recs.append(r)
    T._write_dataset(str(tmp_path), recs)
    out = subprocess.run([sys.executable, SLOW, "--data", str(tmp_path), "--estimator-label", "x-grasp"],
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "2 queries" in out.stdout and "1 queries at least" in out.stdout
    assert "+890 ms" in out.stdout and "select 1" in out.stdout and "select 2" not in out.stdout
    assert "Hash Join" in out.stdout and "worst estimates" in out.stdout
    out2 = subprocess.run([sys.executable, SLOW, "--data", str(tmp_path), "--estimator-label", "x-grasp",
                           "--baseline-label", "x-postgres"], capture_output=True, text=True)
    assert out2.returncode == 0, out2.stderr
    assert out2.stdout == out.stdout
