"""NNGP plugin: kernel, encoding, posterior, abstention (no database)."""

import math
import os
import sys
from types import SimpleNamespace

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pgpi  # noqa: E402
from pgpi.estimator_plugins import nngp  # noqa: E402
from pgpi.request import Request  # noqa: E402

T = {"alias": "t", "rel": "public.t", "rti": 1}
U = {"alias": "u", "rel": "public.u", "rti": 2}


def base(clauses):
    return Request({"point": "base", "rels": [T], "outer": [], "outer_clauses": [], "opaque": 0, "nonrel": False,
                    "jointype": 0, "pg": 10.0, "clauses": [{"sql": s, "rels": "t"} for s in clauses]})


def join(clauses):
    return Request({"point": "join", "rels": [T, U], "outer": [], "outer_clauses": [], "opaque": 0, "nonrel": False,
                    "jointype": 0, "pg": 10.0,
                    "clauses": [{"sql": "(t.id = u.t_id)", "rels": "t u"}] + [{"sql": s, "rels": "t"} for s in clauses]})


class DS:
    def __init__(self, pairs):
        self.pairs = pairs

    def examples(self):
        return [SimpleNamespace(request=r, true_rows=y) for r, y in self.pairs]


def test_relu_kernel_matches_monte_carlo():
    rng = np.random.default_rng(0)
    cov = np.array([[1.0, 0.6], [0.6, 2.0]])
    s = rng.multivariate_normal([0, 0], cov, size=400000)
    mc = float(np.mean(np.maximum(s[:, 0], 0) * np.maximum(s[:, 1], 0)))
    exact = float(nngp.relu_expectation(np.array(0.6), np.array(1.0), np.array(2.0)))
    assert abs(mc - exact) < 0.01
    X = rng.random((30, 5))
    K = nngp.nngp_kernel(X, X)
    assert np.allclose(K, K.T) and np.linalg.eigvalsh(K).min() > -1e-9      # a kernel


def workload():
    pairs = []
    for v in range(0, 100, 5):                      # rows = 10 v + 10, ranges on a; b = 'x' halves them
        pairs.append((base([f"(t.a < '{v}'::integer)"]), 10.0 * v + 10))
        pairs.append((base([f"(t.a < '{v}'::integer)", "((t.b)::text = 'x'::text)"]), 5.0 * v + 5))
        pairs.append((join([f"(t.a < '{v}'::integer)"]), 30.0 * v + 30))
    return pairs


def test_encoding_and_posterior():
    e = pgpi.load("nngp", {})
    e.fit(DS(workload()))
    enc = e.encoder
    assert enc.kind[("public.t", "a")] == "num" and enc.kind[("public.t", "b")] == "cat"
    x = enc.encode(base(["(t.a < '50'::integer)", "((t.b)::text = 'x'::text)"]))
    # relations t, u; a: [0, 0.5]; b: bitmap of {x} + other; join pair: 000
    assert list(x) == [1.0, 0.0, 0.0, 50 / 95, 1.0, 0.0, 0.0, 0.0, 0.0]
    mu, sd = e.predict(base(["(t.a < '50'::integer)"]))
    assert abs(math.exp(mu) - 510) / 510 < 0.1                                  # observed: close
    near = e.predict(base(["(t.a < '52'::integer)"]))
    assert abs(math.exp(near[0]) - 530) / 530 < 0.2                             # between two observations
    far = e.predict(base(["(t.a < '50'::integer)", "((t.b)::text = 'y'::text)"]))
    assert far[1] > near[1]                                                    # an unseen value: less certain
    assert e.estimate(join(["(t.a < '50'::integer)"]))["rows"] > 1000           # the join is told apart


def test_abstains_outside_the_encoding(monkeypatch):
    e = pgpi.load("nngp", {})
    e.fit(DS(workload()))
    assert e.estimate(base(["((t.b)::text ~~ 'x%'::text)"])) is None           # LIKE
    assert e.estimate(base(["(t.c < '3'::integer)"])) is None                  # an attribute never seen
    u_alone = Request({"point": "base", "rels": [U], "outer": [], "outer_clauses": [], "opaque": 0,
                       "nonrel": False, "jointype": 0, "pg": 10.0, "clauses": []})
    assert e.encoder.encode(u_alone) is not None and e.estimate(u_alone) is None   # u alone: never observed
    monkeypatch.setattr(nngp, "SEEN_RELATIONS", False)
    assert e.estimate(u_alone) is not None
    monkeypatch.setattr(nngp, "MAX_CV", 1e-9)
    assert e.estimate(base(["(t.a < '50'::integer)"])) is None                 # too uncertain
