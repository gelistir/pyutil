import numpy as np
import cvxpy as cvx
import pandas as pd


def minimize(objective, constraints=None):
    return cvx.Problem(cvx.Minimize(objective), constraints).solve()


def maximize(objective, constraints=None):
    return cvx.Problem(cvx.Maximize(objective), constraints).solve()


def std(vector):
    # we assume here that the vector is centered
    return cvx.norm(vector, 2)/np.sqrt(vector.size-1)


def elasticNet(matrix, w0, lamb_balance=0, lamb_trades=0):
    w = cvx.Variable(matrix.shape[1])
    objective = cvx.norm(matrix.values*w,2) + lamb_balance*cvx.norm(w,2) + lamb_trades*cvx.norm(w-w0,1)
    minimize(objective=objective, constraints=[0 <= w, cvx.sum(w) == 1])
    return pd.Series(index=matrix.keys(), data=w.value)

def mean_variation(ts):
    return ts.diff().abs().mean()


def signals(matrix, r, lamb=0.0):
    x = cvx.Variable(matrix.shape[1])
    D = np.diag(matrix.apply(mean_variation))
    minimize(objective=cvx.norm(matrix.values * x - r) + lamb*cvx.norm(D*x, 1))
    return pd.Series(index=matrix.keys(), data=x.value)
