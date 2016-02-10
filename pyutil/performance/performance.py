import pandas as pd
import numpy as np
from collections import OrderedDict
from pyutil.performance.drawdown import drawdown
from pyutil.performance.periods import periods, period_returns
from pyutil.performance.var import value_at_risk, conditional_value_at_risk


def __gmean(a):
    return np.exp(np.mean(np.log(a)))


def __cumreturn(ts):
    return (ts + 1.0).prod() - 1.0


def performance(nav, days=262, years=None):
    assert isinstance(nav.index[0], pd.Timestamp)

    r = nav.pct_change().dropna()
    d = OrderedDict()

    a = 100 * __cumreturn(r)

    d["Return"] = a
    d["# Days"] = r.size

    d["Annua. Return"] = 100 * days * (__gmean(r + 1) - 1.0)
    d["Annua. Volatility"] = 100 * np.sqrt(days) * r.std()
    d["Annua. Sharpe Ratio"] = np.sqrt(days) * (__gmean(r + 1) - 1.0) / r.std()

    d["Max Drawdown"] = 100 * drawdown(nav).max()
    d["Max % return"] = 100 * r.max()
    d["Min % return"] = 100 * r.min()

    offsets = periods(today=r.index[-1])
    per = period_returns(returns=r, offset=offsets)

    d["MTD"] = 100*per["Month-to-Date"]
    d["YTD"] = 100*per["Year-to-Date"]

    d["Current Nav"] = nav[r.index[-1]]
    d["Max Nav"] = nav.max()
    d["Current Drawdown"] = 100 * (nav.max() - nav[r.index[-1]])/nav.max()

    period = offsets["Three Years"]

    nav3 = nav.truncate(before=period[0], after=period[1])
    r3 = nav3.pct_change().dropna()
    d["Calmar Ratio (3Y)"] = days * (__gmean(r3 + 1) - 1.0) / drawdown(nav3).max()

    d["# Positive Days"] = r[r > 0].size
    d["# Negative Days"] = r[r < 0].size
    d["Value at Risk (alpha = 0.95)"] = 100*value_at_risk(nav.dropna(), alpha=0.95)
    d["Conditional Value at Risk (alpha = 0.95)"] = 100*conditional_value_at_risk(nav.dropna(), alpha=0.95)
    d["First"] = r.index[0]
    d["Last"] = r.index[-1]

    if years:
        for y in years:
            d[str(y)] = 100 * __cumreturn(r[r.index.year == y])
    return pd.Series(d)

