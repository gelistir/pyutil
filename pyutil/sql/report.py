import pandas as pd


def __portfolios(db, names=None):
    if names:
        return {name: db.PortfolioSQL.get(name=name) for name in names}
    else:
        return {portfolio.name: portfolio for portfolio in db.PortfolioSQL.select()}


def mtd(db, names=None):
    frame = pd.DataFrame({name: portfolio.nav.mtd_series for name, portfolio in __portfolios(db, names).items()}).sort_index(ascending=False)
    frame.index = [a.strftime("%b %d") for a in frame.index]
    frame = frame.transpose()
    frame["total"] = (frame + 1).prod(axis=1) - 1
    return frame                            #.reset_index().rename(columns={"index": "Name"})


def ytd(db, names=None):
    frame = pd.DataFrame({name: portfolio.nav.ytd_series for name, portfolio in __portfolios(db, names).items()}).sort_index(ascending=False)
    frame.index = [a.strftime("%b") for a in frame.index]
    frame = frame.transpose()
    frame["total"] = (frame + 1).prod(axis=1) - 1
    return frame                                            #.reset_index().rename(columns={"index": "Name"})


def sector(db, names=None):
    def f(frame):
        return frame.ix[frame.index[-1]]

    #print({name: f(portfolio.sector) for name, portfolio in __portfolios(names).items()})
    frame = pd.DataFrame({name: f(portfolio.sector) for name, portfolio in __portfolios(db, names).items()}).sort_index(ascending=False)
    frame = frame.transpose()
    frame["total"] = frame.sum(axis=1)
    return frame


def recent(db, names=None, n=15):
    frame = pd.DataFrame({name: portfolio.nav.recent() for name, portfolio in __portfolios(db, names).items()}).sort_index(ascending=False)
    frame.index = [a.strftime("%b %d") for a in frame.index]
    frame = frame.head(n)
    frame = frame.transpose()
    frame["total"] = (frame + 1).prod(axis=1) - 1
    return frame


def period_returns(db, names=None):
    frame = pd.DataFrame({name: portfolio.nav.period_returns for name, portfolio in __portfolios(db, names).items()}).sort_index(ascending=False)
    return frame.transpose()

def performance(db, names=None):
    frame = pd.DataFrame({name: portfolio.nav.summary() for name, portfolio in __portfolios(db, names).items()}).sort_index(ascending=False)
    return frame.transpose()


def reference(db):
    x = pd.DataFrame({symbol.bloomberg_symbol: symbol.reference for symbol in db.Symbol.select()}).transpose()
    x.index.name = "Asset"
    return x


def history(db, field="PX_LAST"):
    frame = pd.DataFrame({x.symbol.bloomberg_symbol: x.series for x in db.Timeseries.select(lambda x: x.name == field)})
    frame.index.name = "Date"
    return frame

