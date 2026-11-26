"""Unit tests that need no PostgreSQL server."""

import json
import os
import socket
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pgpi  # noqa: E402
from pgpi.anonymize import Anonymizer  # noqa: E402
from pgpi.collect import write_manifest, write_part, read_manifest  # noqa: E402
from pgpi.metrics import Split, evaluate, summarize  # noqa: E402
from pgpi.request import Request, answer_rows, strip_literals  # noqa: E402
from pgpi.schema import parallel_divisor, plan_to_rows, q_error  # noqa: E402
from pgpi.server import EstimatorServer  # noqa: E402

D = {"alias": "d", "rel": "public.d", "relid": 1, "rti": 1}
F = {"alias": "f", "rel": "public.f", "relid": 2, "rti": 2}


def join_req(cat="hot", order=0, rti_shift=0):
    cl = [{"sql": f"(d.cat = '{cat}'::text)", "rels": "d"}, {"sql": "(d.id = f.d_id)", "rels": "d f"}]
    if order:
        cl.reverse()
    rels = [dict(D, rti=1 + rti_shift), dict(F, rti=2 + rti_shift)]
    return Request({"point": "join", "rels": rels, "outer": [], "clauses": cl,
                    "outer_clauses": [], "opaque": 0, "nonrel": False, "jointype": 0, "pg": 260})


def test_key_invariance_and_template():
    a, b, c = join_req(), join_req(order=1, rti_shift=5), join_req(cat="cold")
    assert a.key() == b.key()
    assert a.key() != c.key()
    assert a.template() == c.template()
    assert strip_literals("(x.a = 'it''s'::text)") == "(x.a = '?'::text)"


def test_count_and_param_sql():
    a = join_req()
    assert a.count_sql().startswith("SELECT count(*) FROM public.d AS \"d\", public.f AS \"f\" WHERE")
    p = Request({"point": "param_base", "rels": [F], "outer": [D],
                 "clauses": [{"sql": "(d.id = f.d_id)", "rels": "d f"}],
                 "outer_clauses": [{"sql": "(d.cat = 'hot'::text)", "rels": "d"}], "pg": 99})
    num, dens = p.param_sqls()
    assert "public.f AS \"f\", public.d AS \"d\"" in num and len(dens) == 1
    assert p.is_param
    opaque = Request({"point": "base", "rels": [D], "clauses": [{"sql": None, "rels": "d"}], "opaque": 1})
    assert opaque.count_sql() is None


def test_answers():
    r = join_req()
    assert answer_rows(r, {"rows": 5}) == 5
    assert answer_rows(r, {"factor": 2}) == 520
    cl = Request({"point": "clauselist", "rels": [D], "pg": 0.01, "tuples": 1000})
    assert answer_rows(cl, {"sel": 0.1}) == 100
    assert answer_rows(cl, {"factor": 2}) == 20
    assert pgpi.request.normalize_answer(3) == {"rows": 3.0}


def test_q_error_and_divisor():
    assert q_error(10, 100) == 10 and q_error(100, 10) == 10 and q_error(0, 0.5) == 1
    assert abs(parallel_divisor(2) - 2.4) < 1e-9 and parallel_divisor(4) == 4


def _plan_record():
    ce_join = join_req().data
    ce_base = {"point": "base", "rels": [D], "outer": [], "clauses": [ce_join["clauses"][0]],
               "outer_clauses": [], "opaque": 0, "nonrel": False, "jointype": 0, "rel_rows": 13}
    ce_f = {"point": "base", "rels": [F], "outer": [], "clauses": [], "outer_clauses": [],
            "opaque": 0, "nonrel": False, "jointype": 0, "rel_rows": 2000000}
    nodes = [
        {"id": 0, "parent": -1, "role": "top", "type": "Aggregate", "plan_rows": 1, "ntuples": 1, "nloops": 1, "done": 1, "censored": False},
        {"id": 1, "parent": 0, "role": "outer", "type": "Gather", "workers_planned": 2, "plan_rows": 2, "ntuples": 3, "nloops": 1, "done": 1, "censored": False},
        {"id": 2, "parent": 1, "role": "outer", "type": "Hash Join", "point": "join", "plan_rows": 100, "ntuples": 999, "nloops": 3, "done": 3, "censored": False,
         "ce": dict(ce_join, rel_rows=240), "est": {"pg": 240, "answer": None, "source": None, "applied": False, "route": "E"}},
        {"id": 3, "parent": 2, "role": "outer", "type": "Seq Scan", "point": "base", "parallel_aware": True, "plan_rows": 833333, "ntuples": 2000000, "nloops": 3, "done": 3, "censored": False, "ce": ce_f},
        {"id": 4, "parent": 2, "role": "inner", "type": "Hash", "plan_rows": 13, "ntuples": 30, "nloops": 3, "done": 3, "censored": False},
        {"id": 5, "parent": 4, "role": "outer", "type": "Seq Scan", "point": "base", "plan_rows": 13, "ntuples": 30, "nloops": 3, "done": 3, "censored": False, "ce": ce_base},
    ]
    return {"seqid": 7, "label": "t", "queryid": 1, "planid": 2, "status": "complete",
            "nodes": nodes, "meta": {"mode": "off", "stats_refs": [{"relid": 1, "rel": "public.d", "version": 5}],
                                     "requests": [{"seq": 1, "node": 2, "answer": None, "source": None,
                                                   "applied": False, "request": dict(ce_join, route="E")}]}}


def test_plan_to_rows_conventions():
    p, ns, rs = plan_to_rows(_plan_record())
    by = {n["node_id"]: n for n in ns}
    assert p["n_nodes"] == 6 and p["stats_refs"][0]["version"] == 5
    # partial join: total rows against the estimate for the full relation
    assert by[2]["partial"] and by[2]["true_rows"] == 999 and by[2]["est_pg"] == 240
    # parallel-aware scan: plan_rows per worker, estimate scaled by the divisor
    assert by[3]["partial"] and abs(by[3]["est_plan"] - 2000000) < 1
    # inner side of a non-parallel hash: full rows per process
    assert not by[5]["partial"] and by[5]["true_rows"] == 10
    assert by[5]["usable"] and by[5]["q_error_pg"] == 1.3
    assert by[2]["ce_key"] == join_req().key()
    assert rs[0]["key"] == join_req().key()
    # a record logged before 0.10 names the detour route E: read as D
    assert by[2]["est_route"] == "D" and rs[0]["route"] == "D"
    assert Request({"route": "B"}).route == "H" and Request({"route": "D"}).route == "D"


def test_anonymizer_consistency():
    an = Anonymizer("s3cret", "full")
    rq = an.request(join_req().data)
    assert "cat" not in json.dumps(rq) and "public" not in json.dumps(rq)
    rq2 = an.request(join_req(order=1).data)
    assert Request(rq).key() == Request(rq2).key()
    # the same literal maps to the same token, in clauses and in statistics
    tok = an.value("hot")
    assert tok in rq["clauses"][0]["sql"] or tok in rq["clauses"][1]["sql"]
    st = an.stats_record({"rel": "public.d", "database": "db", "stats": json.dumps({
        "name": "public.d", "columns": [{"attname": "cat", "slots": [{"kind": 1, "values": ["hot"], "numbers": [0.5]}]}],
        "indexes": [], "extended": []})})
    s = json.loads(st["stats"])
    assert s["columns"][0]["slots"][0]["values"] == [tok]
    assert s["columns"][0]["attname"] in json.dumps(rq)
    assert Anonymizer("other", "full").value("hot") != tok
    names = Anonymizer("s3cret", "names").request(join_req().data)
    assert "'hot'" in names["clauses"][0]["sql"] + names["clauses"][1]["sql"]


def _write_dataset(path, recs):
    os.makedirs(path, exist_ok=True)
    m = read_manifest(path)
    plans, nodes, reqs = [], [], []
    for r in recs:
        p, n, q = plan_to_rows(r)
        plans.append(p); nodes.extend(n); reqs.extend(q)
    write_part(path, "plans", plans, 1)
    write_part(path, "nodes", nodes, 1)
    write_part(path, "requests", reqs, 1)
    m["parts"] = 1
    write_manifest(path, m)


def test_dataset_and_feedback_evaluation():
    with tempfile.TemporaryDirectory() as tmp:
        recs = []
        for i in range(6):
            r = _plan_record()
            r["seqid"] = i + 1
            r["queryid"] = i
            recs.append(r)
        _write_dataset(tmp, recs)
        ds = pgpi.Dataset(tmp)
        exs = list(ds.examples(points=["join"]))
        assert len(exs) == 6 and exs[0].true_rows == 999
        est = pgpi.load("feedback")
        res = evaluate(ds, [est], points=["join"])
        allg = res["groups"]["all"]
        assert allg["postgres"]["median"] > 4 and allg["feedback"]["median"] == 1.0
        assert res["coverage"]["feedback"] == 1.0
        tc = pgpi.load("truecard")
        tc.fit(ds)
        assert tc.estimate(join_req())["rows"] == 999
        res2 = evaluate(ds, [pgpi.load("feedback")], points=["join"], split=Split(0.5, "queryid"))
        assert res2["examples"] < 6
        assert summarize([1, 2, 4])["median"] == 2


class Five(pgpi.Estimator):
    name = "five"

    def estimate(self, req):
        return 5 if req.point == "join" else None


class Same(pgpi.Estimator):
    name = "same"

    def estimate(self, req):
        return pgpi.factor(1.0)


def test_server_roundtrip():
    path = os.path.join(tempfile.gettempdir(), f"pgpi_test_{os.getpid()}.sock")
    with EstimatorServer([Five(), Same()], path) as srv:
        s = socket.socket(socket.AF_UNIX)
        s.connect(path)
        f = s.makefile("rw")
        f.write(json.dumps(join_req().data) + "\n"); f.flush()
        assert json.loads(f.readline()) == {"rows": 5.0, "source": "five"}
        f.write(json.dumps({"point": "base", "rels": [D]}) + "\n"); f.flush()
        assert json.loads(f.readline()) == {"factor": 1.0, "source": "same"}
        s.close()
        assert srv.stats["requests"] == 2


def test_server_guard():
    srv = EstimatorServer([Five(), Same()], "/nonexistent.sock", guard=10)
    # five answers 5 rows. pg = 260 is 52x away, so the guard stops it and the next estimator answers.
    assert srv.decide(join_req().data) == {"factor": 1.0, "source": "same"}
    assert srv.stats["guarded"] == 1
    near = dict(join_req().data, pg=20)          # 4x away: kept
    assert srv.decide(near) == {"rows": 5.0, "source": "five"}
    assert EstimatorServer([Five()], "/x", guard=10).decide(join_req().data) == {}


def test_server_guard_under():
    # five answers 5 rows: 4x below pg = 20, 52x below pg = 260
    near = dict(join_req().data, pg=20)
    assert EstimatorServer([Five()], "/x", guard=10, guard_under=3).decide(near) == {}
    assert EstimatorServer([Five()], "/x", guard_under=10).decide(near) == {"rows": 5.0, "source": "five"}
    above = dict(join_req().data, pg=0.5)           # 10x above: only --guard checks above
    assert EstimatorServer([Five()], "/x", guard_under=3).decide(above) == {"rows": 5.0, "source": "five"}
    assert EstimatorServer([Five()], "/x", guard=3, guard_under=100).decide(above) == {}


def test_plugin_loading_from_file():
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, "myest.py")
        with open(p, "w") as f:
            f.write("import pgpi\nclass Mine(pgpi.Estimator):\n    name='mine'\n"
                    "    def estimate(self, req):\n        return pgpi.factor(float(self.arg or 1))\n")
        est = pgpi.load(f"{p}:Mine:3")
        assert est.name == "mine" and est.estimate(join_req()) == {"factor": 3.0}
