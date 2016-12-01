import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay


def __index(t0, t1):
    return pd.DatetimeIndex(freq=BDay(), start=pd.Timestamp(t0).date(), end=pd.Timestamp(t1).date())

def __toarray(x):
    if isinstance(x, list):
        return np.array(x)

def __default(x, value):
    if isinstance(x, np.ndarray):
        return x
    else:
        return value


def rand_asset(sigma=0.01, drift=0.0, t0="2010-01-01", t1="today"):
    x = __index(t0, t1)
    r = pd.Series(index=x, data=drift + sigma * np.random.randn(x.shape[0]))
    return (1 + r).cumprod()



def rand_assets(n, sigma=None, corr=None, drift=None, t0="2010-01-01", t1="today"):
    x = __index(t0, t1)

    sigma = __toarray(sigma)
    drift = __toarray(drift)
    corr = __toarray(corr)

    sigma = __default(sigma, 0.01*np.ones((n,)))
    drift = __default(drift, np.zeros((n,)))
    corr = __default(corr, np.eye(n,n))

    cov = np.diag(sigma) @ corr @ np.diag(sigma)
    r = np.random.multivariate_normal(mean=drift, cov=cov, size=(x.shape[0],))
    return pd.DataFrame(index=x,data=r+1).cumprod(axis=0)
