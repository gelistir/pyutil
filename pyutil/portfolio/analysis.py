import pandas as pd
from pyutil.performance.summary import fromNav


def __last(frame, datefmt=None):
    frame = frame.sort_index(axis=1, ascending=False)
    if datefmt:
        frame = frame.rename(columns=lambda x: x.strftime(datefmt))
    frame["total"] = (frame + 1).prod(axis=1) - 1
    frame.index.name = "Portfolio"
    return frame


def __percentage(x):
    return "{0:.2f}%".format(float(100.0 * x)).replace("nan%", "")


def nav(portfolios, f=lambda x: x):
    return pd.DataFrame({name: f(portfolio.nav) for name, portfolio in portfolios.items()})


def mtd(portfolios):
    assert isinstance(portfolios, dict)
    frame = nav(portfolios=portfolios, f=lambda x: fromNav(x).returns)
    today = frame.index[-1]
    offset = -2 if today.day < 5 else -1
    first_day_of_month = (today + pd.offsets.MonthBegin(offset)).date()

    frame = frame.truncate(before=first_day_of_month)
    return __last(frame.transpose(), datefmt="%b %d").applymap(__percentage)


def ytd(portfolios):
    assert isinstance(portfolios, dict)
    frame = nav(portfolios=portfolios, f=lambda x: fromNav(x).ytd_series).transpose()
    return __last(frame).applymap(__percentage)


def recent(portfolios, n=15):
    # define the function
    assert isinstance(portfolios, dict)
    frame = nav(portfolios=portfolios, f=lambda x: fromNav(x).recent(n)).tail(n).transpose()
    return __last(frame, datefmt="%b %d").applymap(__percentage)


def sector(portfolios, symbols, total=False):
    frame = pd.DataFrame({name: portfolio.sector(symbols, total=total).iloc[-1] for name, portfolio in portfolios.items()}).transpose()
    frame.index.name = "Portfolio"
    return frame.applymap(__percentage)
