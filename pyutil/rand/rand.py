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


def rand_asset_auto(corr=0, mu=0, sigma=0.01, t0="2010-01-01", t1="today"):
    x = __index(t0, t1)

    assert -1 < corr < 1, "Auto-correlation must be between -1 and 1"

    # Find out the offset `c` and the std of the white noise `sigma_e`
    # that produce a signal with the desired mean and variance.
    # See https://en.wikipedia.org/wiki/Autoregressive_model#Example:_An_AR.281.29_process
    c = mu * (1 - corr)
    sigma_e = np.sqrt((sigma ** 2) * (1 - corr ** 2))

    # Sample the auto-regressive process.
    signal = [c + np.random.normal(0, sigma_e)]
    for _ in range(1, len(x)):
        signal.append(c + corr * signal[-1] + np.random.normal(0, sigma_e))

    r = pd.Series(index=x, data=signal)
    return (r+1).cumprod()
