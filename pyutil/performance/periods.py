import pandas as pd
from collections import namedtuple, OrderedDict

Period = namedtuple("Period", ["start", "end"])


def __cumreturn(ts):
    return (ts + 1.0).prod() - 1.0


def periods(today=None):
    today = today or pd.Timestamp("today")

    def __f(offset, today):
        return Period(start=today - offset, end=today)

    offset = pd.Series()
    offset["Two weeks"] = pd.DateOffset(weeks=2)
    offset["Month-to-Date"] = pd.offsets.MonthBegin()
    offset["Year-to-Date"] = pd.offsets.YearBegin()
    offset["One Month"] = pd.DateOffset(months=1)
    offset["Three Months"] = pd.DateOffset(months=3)
    offset["One Year"] = pd.DateOffset(years=1)
    offset["Three Years"] = pd.DateOffset(years=3)
    offset["Five Years"] = pd.DateOffset(years=5)
    offset["Ten Years"] = pd.DateOffset(years=10)

    return offset.apply(__f, today=today)


def period_returns(returns, offset=None):
    """

    :param r: time series of returns
    :param offset: periods
    :return:
    """
    if not isinstance(offset, pd.Series):
        offset = periods()

    assert isinstance(returns.index[0], pd.Timestamp)
    r = returns.dropna()
    p_returns = OrderedDict()

    for key, period in offset.iteritems():
        assert isinstance(period, Period)
        p_returns[key] = __cumreturn(r.truncate(before=period.start, after=period.end))

    return pd.Series(p_returns)



