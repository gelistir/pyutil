import pandas as pd
import numpy as np
from collections import OrderedDict
from .drawdown import drawdown
from .periods import periods, period_returns
from .var import value_at_risk, conditional_value_at_risk


def __gmean(a):
    # geometric mean A
    # Prod [a_i] == A^n
    # Apply log on both sides
    # Sum [log a_i] = n log A
    # => A = exp(Sum [log a_i] // n)
    return np.exp(np.mean(np.log(a)))


def __cumreturn(ts):
    return (1 + ts).prod() - 1.0


def sharpe_ratio(nav, days=262):
    # compute one day returns
    r = nav.resample("1D").last().pct_change().dropna()
    return np.sqrt(days) * (__gmean(r + 1) -1.0) / r.std()


def sortino_ratio(nav, days=262):
    start = nav.index[-1] - pd.DateOffset(years=3)
    # truncate the nav
    x = nav.truncate(before=start)
    # compute the returns for that period
    r = x.resample("1D").last().pct_change().dropna()
    return days*(__gmean(r + 1) - 1.0)/drawdown(x).max()


def annualized_return(nav, days=262):
    r = nav.resample("1D").last().pct_change().dropna()
    return 100 * days * (__gmean(r + 1)-1.0)


def annualized_volatility(nav, days=262):
    r = nav.resample("1D").last().pct_change().dropna()
    return 100 * np.sqrt(days) * r.std()


def performance(nav, days=262, years=None):
    assert isinstance(nav.index[0], pd.Timestamp)

    r = nav.resample("1D").last().pct_change().dropna()
    last = nav.dropna().index[-1]

    d = OrderedDict()

    d["Return"] = 100 * __cumreturn(r)
    d["# Days"] = r.size

    d["Annua. Return"] = annualized_return(nav, days=days)
    d["Annua. Volatility"] = annualized_volatility(nav, days=days)
    d["Annua. Sharpe Ratio"] = sharpe_ratio(nav, days=days)

    dd = drawdown(nav)
    d["Max Drawdown"] = 100 * dd.max()
    d["Max % return"] = 100 * r.max()
    d["Min % return"] = 100 * r.min()

    per = period_returns(returns=r, offset=periods(today=r.index[-1]))

    d["MTD"] = 100*per["Month-to-Date"]
    d["YTD"] = 100*per["Year-to-Date"]

    d["Current Nav"] = nav[last]
    d["Max Nav"] = nav.max()
    d["Current Drawdown"] = 100 * dd[dd.index[-1]] #(nav.max() - nav[last])/nav.max()

    d["Calmar Ratio (3Y)"] = sortino_ratio(nav, days=days)

    d["Positive Days"] = r[r > 0].size
    d["Negative Days"] = r[r < 0].size
    d["Value at Risk (alpha = 0.95)"] = 100*value_at_risk(nav.dropna(), alpha=0.95)
    d["Conditional Value at Risk (alpha = 0.95)"] = 100*conditional_value_at_risk(nav.dropna(), alpha=0.95)
    d["First"] = r.index[0]
    d["Last"] = r.index[-1]

    if years:
        for y in years:
            d[str(y)] = 100 * __cumreturn(r[r.index.year == y])

    return pd.Series(d)

