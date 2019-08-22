import pandas as pd

from pyutil.performance.summary import NavSeries


def __last(frame, datefmt=None):
    frame = frame.sort_index(axis=1, ascending=False)
    if datefmt:
        frame = frame.rename(columns=lambda x: x.strftime(datefmt))
    frame["total"] = (frame + 1).prod(axis=1) - 1
    frame.index.name = "Portfolio"
    return frame


def nav(portfolios) -> pd.DataFrame:
    assert isinstance(portfolios, dict)
    frame = pd.DataFrame({name: portfolio.nav for name, portfolio in portfolios.items()})
    frame.columns.name = "Portfolio"
    return frame


def mtd(frame: pd.DataFrame) -> pd.DataFrame:
    x = frame.apply(lambda x: NavSeries(x).mtd_series).transpose()
    return __last(x, datefmt="%b %d")


def ytd(frame: pd.DataFrame) -> pd.DataFrame:
    x = frame.apply(lambda x: NavSeries(x).ytd_series).transpose()
    return __last(x, datefmt="%m")


def recent(frame: pd.DataFrame, n=15) -> pd.DataFrame:
    x = frame.apply(lambda x: NavSeries(x).recent(n=n)).tail(n).transpose()
    return __last(x, datefmt="%b %d")


def sector(portfolios, symbolmap, total=False) -> pd.DataFrame:
    assert isinstance(portfolios, dict)
    return pd.DataFrame({name : portfolio.sector(symbolmap=symbolmap, total=total).iloc[-1] for name, portfolio in portfolios.items()})


def performance(frame: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return frame.apply(lambda x: NavSeries(x).summary_format(**kwargs))


def drawdown(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.apply(lambda x: NavSeries(x).drawdown)


def ewm_volatility(frame: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return frame.apply(lambda x: NavSeries(x).ewm_volatility(**kwargs))
