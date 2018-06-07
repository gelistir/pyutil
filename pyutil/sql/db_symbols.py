import pandas as pd

from pyutil.performance.summary import fromNav
from pyutil.portfolio.portfolio import Portfolio as PP
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.session import session as sss
from pyutil.sql.util import to_pandas, parse


class Database(object):
    def __init__(self, session=None):
        self.__session = session or sss(db="symbols")

    @property
    def nav(self):
        x = pd.read_sql_query("SELECT * FROM v_portfolio_nav", con=self.__session.bind, index_col="name")["data"]
        return x.apply(to_pandas)

    def sector(self, total=False):
        frame = pd.read_sql_query("SELECT * FROM v_portfolio_sector", con=self.__session.bind, index_col=["name", "symbol", "group"])["data"]
        frame = frame.apply(to_pandas).groupby(level=["name", "group"], axis=0).sum().ffill(axis=1)
        if total:
            frame["total"] = frame.sum(axis=1)
        return frame

    @property
    def mtd(self):
        frame = self.nav.apply(lambda x: fromNav(x).mtd_series, axis=1)
        frame = frame.rename(columns=lambda x: x.strftime("%b %d"))
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    @property
    def ytd(self):
        #print(self.nav)
        frame = self.nav.apply(lambda x: fromNav(x).ytd_series, axis=1)
        #print(frame)
        #assert False

        frame = frame.rename(columns=lambda x: x.strftime("%b"))
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    def recent(self, n=15):
        frame = self.nav.apply(lambda x: fromNav(x).recent(n=n), axis=1).iloc[:, -n:]
        frame = frame.rename(columns=lambda x: x.strftime("%b %d"))
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    @property
    def period_returns(self):
        return self.nav.apply(lambda x: fromNav(x).period_returns, axis=1)


    @property
    def performance(self):
        return self.nav.apply(lambda x: fromNav(x).summary(), axis=1).transpose()

    def frames(self, total=False, n=15):
        return {"recent": self.recent(n=n),
                "ytd": self.ytd,
                "mtd": self.mtd,
                "sector": self.sector(total=total),
                "periods": self.period_returns,
                "performance": self.performance}

    def portfolio(self, name):
        x = pd.read_sql_query("SELECT * FROM v_portfolio_2 where name=%(name)s", params={"name": name}, con=self.__session.bind, index_col=["timeseries", "symbol"])["data"]
        x = x.apply(to_pandas)
        portfolio = PP(prices=x.loc["price"].transpose(), weights=x.loc["weight"].transpose())
        return portfolio

    def state(self, name):
        portfolio = self.portfolio(name=name)
        reference = pd.read_sql_query(sql="SELECT * FROM v_symbols_state", con=self.__session.bind, index_col=["symbol"])

        frame = pd.concat([portfolio.state, reference.loc[portfolio.assets]], axis=1)

        sector_weights = frame.groupby(by="group")["Extrapolated"].sum()
        frame["Sector Weight"] = frame["group"].apply(lambda x: sector_weights[x])
        frame["Relative Sector"] = 100 * frame["Extrapolated"] / frame["Sector Weight"]
        frame["Asset"] = frame.index
        return frame

    @property
    def states(self):
        for portfolio in self.portfolios:
            yield portfolio.name, self.state(name=portfolio.name)

    @property
    def reference_symbols(self):
        reference = pd.read_sql_query(sql="SELECT * FROM v_reference_symbols", con=self.__session.bind, index_col=["symbol", "field"])

        if reference.empty:
            return pd.DataFrame(index=reference.index, columns=["value"])

        reference["value"] = reference[['content', 'result']].apply(lambda x: parse(x[0], x[1]), axis=1)
        return reference.unstack()["value"]

    def prices(self, name="PX_LAST"):
        prices = pd.read_sql_query(sql="SELECT * FROM v_symbols", con=self.__session.bind, index_col="name")
        prices = prices[prices["timeseries"] == name]
        prices = prices["data"].apply(to_pandas).transpose()
        prices.index.names = ["Date"]
        return prices

    def symbol(self, name):
        return self.__session.query(Symbol).filter_by(name=name).one()

    @property
    def strategies(self):
        for s in self.__session.query(Strategy):
            yield s

    @property
    def portfolios(self):
        for p in self.__session.query(Portfolio):
            yield p

    #def portfolio(self, name):
    #    return self.__session.query(Portfolio).filter_by(name=name).one()
