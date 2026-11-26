"""Tests of the GRASP port (skipped without PyTorch)."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

torch = pytest.importorskip("torch")

import pgpi  # noqa: E402
from pgpi.dataset import Example  # noqa: E402
from pgpi.estimator_plugins.grasp import ArCDF, rq_spline  # noqa: E402
from pgpi.predicates import decompose, parse_single  # noqa: E402
from pgpi.request import Request  # noqa: E402

A = {"alias": "a", "rel": "public.a", "rti": 1}
B = {"alias": "b", "rel": "public.b", "rti": 2}


def base_a(v, tuples=1000):
    return {"point": "base", "rels": [A], "outer": [], "outer_clauses": [], "opaque": 0, "jointype": 0,
            "clauses": [{"sql": f"(a.x = '{v}'::text)", "rels": "a"}], "tuples": tuples}


def base_b(lo=None):
    cl = [] if lo is None else [{"sql": f"(b.n >= '{lo}'::integer)", "rels": "b"}]
    return {"point": "base", "rels": [B], "outer": [], "outer_clauses": [], "opaque": 0, "jointype": 0,
            "clauses": cl, "tuples": 10000}


def join_ab(v, lo=None):
    cl = [{"sql": f"(a.x = '{v}'::text)", "rels": "a"}, {"sql": "(a.id = b.a_id)", "rels": "a b"}]
    if lo is not None:
        cl.append({"sql": f"(b.n >= '{lo}'::integer)", "rels": "b"})
    return {"point": "join", "rels": [A, B], "outer": [], "outer_clauses": [], "opaque": 0,
            "jointype": 0, "clauses": cl}


def param_b(v):
    return {"point": "param_base", "rels": [B], "outer": [A], "opaque": 0, "jointype": 0,
            "clauses": [{"sql": "(a.id = b.a_id)", "rels": "a b"}],
            "outer_clauses": [{"sql": f"(a.x = '{v}'::text)", "rels": "a"}], "tuples": 10000}


class FakeDataset:
    def __init__(self, exs):
        self.exs = exs

    def examples(self, points=None, usable_only=True, **kw):
        return iter([e for e in self.exs if not (usable_only and e.censored)])


def ex(data, rows, i, censored=False):
    return Example(request=Request(data), true_rows=rows, est_pg=None, est_plan=None, seqid=i,
                   node_id=0, label="t", queryid=i, planid=0, censored=censored, nloops=1)


def make_examples():
    exs = []
    i = 0
    for k in range(12):
        sel = 10 if k % 3 == 0 else 1          # a.x values with 10 or 1 rows
        exs.append(ex(base_a(f"v{k}"), sel, i)); i += 1
        exs.append(ex(join_ab(f"v{k}"), sel * 10, i)); i += 1     # 10 b rows per a row
        exs.append(ex(param_b(f"v{k}"), 10, i)); i += 1
    for lo in (0, 2000, 5000, 8000):
        exs.append(ex(base_b(lo), 10000 - lo, i)); i += 1
    return exs


def test_spline_monotone_and_bounded():
    torch.manual_seed(0)
    p = torch.randn(1, 3 * 8 - 1).repeat(101, 1)
    x = torch.linspace(0, 1, 101)
    y = rq_spline(x, p, 8)
    assert abs(y[0].item()) < 1e-6 and abs(y[-1].item() - 1) < 1e-6
    assert (y[1:] - y[:-1] >= -1e-7).all()
    m = ArCDF(2, 4)
    lo, hi = torch.tensor([[0.2, 0.0], [0.0, 0.0]]), torch.tensor([[0.7, 1.0], [1.0, 1.0]])
    s = torch.exp(m.log_box(lo, hi, torch.zeros(2, 4)))
    assert 0 < s[0].item() <= 1 and abs(s[1].item() - 1) < 1e-5


def test_decompose_param_and_abstain():
    comp, why = decompose(Request(param_b("v1")))
    assert why is None and [a for a, _ in comp.targets] == ["b"] and [a for a, _ in comp.outers] == ["a"]
    assert comp.groups == [[("a", "id"), ("b", "a_id")]]
    bad = dict(join_ab("v1"), clauses=[{"sql": "(a.id < b.a_id)", "rels": "a b"}])
    assert decompose(Request(bad))[0] is None
    assert decompose(Request(dict(join_ab("v1"), jointype=1)))[0] is None
    assert parse_single("(b.n >= '5'::integer)", "b").is_range


def test_grasp_learns_and_composes():
    est = pgpi.load("grasp:epochs=250,n_lcs=16,n_h=32,lr=0.01,restarts=2")
    est.fit(FakeDataset(make_examples()))
    assert est.history[-1] < est.history[0] / 5
    # seen questions
    for k, sel in ((0, 10), (1, 1)):
        j = est.estimate(Request(join_ab(f"v{k}")))["rows"]
        assert sel * 10 / 3 < j < sel * 10 * 3
    p = est.estimate(Request(param_b("v1")))["rows"]
    assert 10 / 3 < p < 30
    # a range on b never seen together with the join: composed from primitives
    j2 = est.estimate(Request(join_ab("v0", lo=5000)))
    assert j2 is not None and j2["rows"] > 0
    # unknown table: abstain
    other = {"point": "base", "rels": [{"alias": "z", "rel": "public.z"}], "clauses": [], "tuples": 5}
    assert est.estimate(Request(other)) is None


def test_final_labels_and_censored():
    exs = make_examples()
    exs.append(ex(join_ab("v99"), 50, 999, censored=True))
    est = pgpi.load("grasp:labels=final,epochs=5,censored=1,restarts=1")
    chosen, lower = est._training_set(FakeDataset(exs))
    assert all(e.request.n_rels == 2 or e.request.point == "base" for e in chosen)
    assert est.cfg["n_lcs"] == 2048 and pgpi.load("grasp:labels=final,n_lcs=32").cfg["n_lcs"] == 32
    est2 = pgpi.load("grasp:epochs=5,censored=1,restarts=1")
    assert est2.cfg["n_lcs"] == 64
    chosen2, lower2 = est2._training_set(FakeDataset(exs))
    assert len(lower2) == 1 and len(chosen2) == len(exs) - 1
    est2.fit(FakeDataset(exs))
    assert est2.n_train == len(exs)
