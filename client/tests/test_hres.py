"""hres plugin: pooling, uncertainty, asymmetry, local join residuals (no database)."""

import math
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pgpi  # noqa: E402
from pgpi.estimator_plugins import hres  # noqa: E402
from pgpi.request import Request  # noqa: E402

T = {"alias": "t", "rel": "public.t", "relid": 1, "rti": 1}
U = {"alias": "u", "rel": "public.u", "relid": 2, "rti": 2}


def base(v, col="a", rel=T, pg=100.0):
    a = rel["alias"]
    return Request({"point": "base", "rels": [rel], "outer": [], "outer_clauses": [], "opaque": 0, "nonrel": False,
                    "jointype": 0, "pg": pg, "clauses": [{"sql": f"({a}.{col} < '{v}'::integer)", "rels": a}]})


def join(v, pg=1000.0):
    return Request({"point": "join", "rels": [T, U], "outer": [], "outer_clauses": [], "opaque": 0, "nonrel": False,
                    "jointype": 0, "pg": pg, "clauses": [{"sql": f"(t.a < '{v}'::integer)", "rels": "t"},
                                                         {"sql": "(t.id = u.t_id)", "rels": "t u"}]})


def ex(req, true, pg, seqid=1, node=0):
    return SimpleNamespace(request=req, true_rows=true, est_pg=pg, seqid=seqid, node_id=node)


def fitted(exs, monkeypatch, trees=None):
    monkeypatch.setattr(hres, "plan_trees", lambda ds, ids: trees or {})
    e = pgpi.load("hres", {})
    e.fit(SimpleNamespace(examples=lambda: exs))
    return e


def factor(e, req):
    a = e.estimate(req)
    return a["factor"] if a else None


def test_registered():
    assert "hres" in pgpi.registered()


def test_pooling_and_abstention(monkeypatch):
    # the same question seen 8 times, always 4x PostgreSQL's estimate
    e = fitted([ex(base(5), 400, 100, seqid=i) for i in range(8)], monkeypatch)
    f = factor(e, base(5))
    assert 3.0 < f < 4.0                                   # pooled towards 1, never beyond the observed 4
    g = factor(e, base(6))                                  # another literal: the template (L3), no L4
    assert g is not None and g < f * 1.0001 and e.estimate(base(6))["source"] == "hres-l3"
    assert e.estimate(base(5, rel=U)) is None               # another table: nothing known
    # seen once (4x): nothing tells how reliable it is (SIGMA0), so there is almost no correction
    once = factor(fitted([ex(base(5), 400, 100)], monkeypatch), base(5))
    assert once is None or once < 1.2


def test_uncertainty_and_asymmetry(monkeypatch):
    up = [ex(base(v), 100 * f, 100, seqid=v) for v, f in zip(range(8), (2, 8, 2, 8, 2, 8, 2, 8))]
    down = [ex(base(v), 100 / f, 100, seqid=v) for v, f in zip(range(8), (2, 8, 2, 8, 2, 8, 2, 8))]
    fu, fd = factor(fitted(up, monkeypatch), base(99)), factor(fitted(down, monkeypatch), base(99))
    assert fu is not None and 1.0 < fu < 4.0                # 4x on average, spread 2x..8x: only part is certain
    assert fd is None or 1 / fu < fd < 1.0                  # the same evidence downwards is trusted less
    wide = [ex(base(v), 100 * f, 100, seqid=v) for v, f in zip(range(8), (0.01, 100, 0.01, 100, 0.01, 100, 0.01, 100))]
    assert fitted(wide, monkeypatch).estimate(base(99)) is None   # too spread to say anything


def test_join_residual_is_local(monkeypatch):
    # every plan: t is underestimated 4x (100 -> 400), u is exact, and the join is also exactly 4x:
    # the selectivity of the join itself is right, so the join needs no correction of its own
    exs, trees = [], {}
    for s in range(8):
        exs += [ex(join(s), 4000, 1000, s, 0), ex(base(s), 400, 100, s, 1),
                ex(base(s, col="b", rel=U), 50, 50, s, 2)]
        trees[s] = {"children": {0: [("outer", 1), ("inner", 2)]}, "point": {0: "join", 1: "base", 2: "base"},
                    "input": {1: (("t",), 100.0), 2: (("u",), 50.0)}}
    e = fitted(exs, monkeypatch, trees)
    assert factor(e, base(3)) > 3.0 and e.estimate(join(3)) is None
    monkeypatch.setattr(hres, "JOIN_LOCAL", False)          # the total residual corrects the input twice
    assert factor(fitted(exs, monkeypatch, trees), join(3)) > 3.0
    assert math.isclose(hres.TAU, 2.0)
