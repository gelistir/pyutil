import pandas as pd


class Period(object):
    """
    Simple Period model, representing an interval of two stamps
    """
    def __init__(self, start, end):
        assert start < end
        self.__start = start
        self.__end = end

    def apply_to(self, ts):
        return ts.truncate(before=self.__start, after=self.__end)

    def __repr__(self):
        return "Period with start {0} and end {1}".format(self.__start, self.__end)

    @property
    def start(self):
        return self.__start

    @property
    def end(self):
        return self.__end


def __cumreturn(ts):
    return (ts + 1.0).prod() - 1.0


def periods(today=None):
    """
    Construct a series of Period objects

    :param today: If not specified use today's date. Specifying a today is quite useful in unit tests.
    :return:
    """
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
    Compute the returns achieve over certain periods

    :param returns: time series of returns
    :param offset: periods given as a Series, if not specified use standard set of periods
    :return: Series of periods returns, same order as in the period Series
    """
    if not isinstance(offset, pd.Series):
        offset = periods()

    assert isinstance(returns.index[0], pd.Timestamp)
    p_returns = {key: __cumreturn(period.apply_to(returns)) for key, period in offset.iteritems()}

    # preserve the order of the elements in the offset series
    return pd.Series(p_returns).ix[offset.index]




