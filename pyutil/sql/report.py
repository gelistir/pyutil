import pandas as pd

from pyutil.sql.models import PortfolioSQL, Symbol, SymbolGroup


def __portfolios(session, names=None):
    if names:
        return {name: session.query(PortfolioSQL).filter(PortfolioSQL.name==name).first() for name in names}
    else:
        return {portfolio.name: portfolio for portfolio in session.query(PortfolioSQL)}


def mtd(session, names=None):
    frame = pd.DataFrame({name: portfolio.nav.mtd_series for name, portfolio in __portfolios(session, names).items()}).sort_index(ascending=False)
    frame.index = [a.strftime("%b %d") for a in frame.index]
    frame = frame.transpose()
    frame["total"] = (frame + 1).prod(axis=1) - 1
    return frame

def ytd(session, names=None):
    frame = pd.DataFrame({name: portfolio.nav.ytd_series for name, portfolio in __portfolios(session, names).items()}).sort_index(ascending=False)
    frame.index = [a.strftime("%b") for a in frame.index]
    frame = frame.transpose()
    frame["total"] = (frame + 1).prod(axis=1) - 1
    return frame

                                  #.reset_index().rename(columns={"index": "Name"})


def sector(session, names=None):
    def f(frame):
        return frame.iloc[-1]

    map = {symbol.bloomberg_symbol : symbol.group.name for symbol in session.query(Symbol)}

    frame = pd.DataFrame({name: f(portfolio.sector(map=map)) for name, portfolio in __portfolios(session, names).items()}).sort_index(ascending=False)
    frame = frame.transpose()
    frame["total"] = frame.sum(axis=1)
    return frame


def recent(session, names=None, n=15):
    frame = pd.DataFrame({name: portfolio.nav.recent() for name, portfolio in __portfolios(session, names).items()}).sort_index(ascending=False)
    frame.index = [a.strftime("%b %d") for a in frame.index]
    frame = frame.head(n)
    frame = frame.transpose()
    frame["total"] = (frame + 1).prod(axis=1) - 1
    return frame


def period_returns(session, names=None):
    frame = pd.DataFrame({name: portfolio.nav.period_returns for name, portfolio in __portfolios(session, names).items()}).sort_index(ascending=False)
    return frame.transpose()

def performance(session, names=None):
    frame = pd.DataFrame({name: portfolio.nav.summary() for name, portfolio in __portfolios(session, names).items()}).sort_index(ascending=False)
    return frame.transpose()


def reference(session):
    x = pd.DataFrame({symbol.bloomberg_symbol: symbol.reference for symbol in db.Symbol.select()}).transpose()
    x.index.name = "Asset"
    return x


def history(session, field="PX_LAST"):
    frame = pd.DataFrame({x.symbol.bloomberg_symbol: x.series for x in db.Timeseries.select(lambda x: x.name == field)})
    frame.index.name = "Date"
    return frame

