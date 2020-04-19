import cvxpy as cvx
import numpy as np
import pytest
from cvxpy.error import ParameterError

from pyutil.cvx.util import Solver


def solver(n):
    weights = cvx.Variable(n, name="weights")

    mu = cvx.Parameter(n, name="exp_return")
    max_leverage = cvx.Parameter(nonneg=True, name="max_leverage", value=0.0)

    constraints = [cvx.sum(weights) == 0, cvx.norm1(weights) <= max_leverage]

    return Solver(problem=cvx.Problem(objective=cvx.Maximize(mu * weights), constraints=constraints))


def f(mu, max_leverage=None, *args, **kwargs):
    n = len(mu)

    # create a suitable solver
    s = solver(n)

    # inject the parameters
    s.parameters["exp_return"].value = mu
    s.parameters["max_leverage"].value = max_leverage or s.parameters["max_leverage"].value

    # solve the problem
    s.solve(*args, **kwargs)

    # return the solution
    return s.variables["weights"].value


def test_solve():
    s = solver(2)

    s.parameters["exp_return"].value = np.array([2.0, 3.0])
    s.parameters["max_leverage"].value = 2.0

    s.solve()
    np.allclose(s.variables["weights"].value, np.array([-1.0, +1.0]))

    d = {name: v for name, v in s.variables.items()}
    np.allclose(d["weights"].value, np.array([-1.0, +1.0]))

    d = {name: p for name, p in s.parameters.items()}
    assert d["max_leverage"].value == 2.0


def test_neg_max_leverage():
    s = solver(2)
    with pytest.raises(ValueError):
        s.parameters["max_leverage"].value = -2.0

    with pytest.raises(ValueError):
        s.parameters["exp_return"].value = np.array([2.0, np.nan])


def test_f():
    weights = f(mu=np.array([2.0, 3.0]), max_leverage=2.0)
    np.allclose(weights, np.array([-1.0, +1.0]))


def test_too_early():
    s = solver(2)
    # if you call the solver without injecting the parameters first... error
    with pytest.raises(ParameterError):
        s.solve()

