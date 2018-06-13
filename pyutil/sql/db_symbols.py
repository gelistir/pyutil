import pandas as pd

from pyutil.performance.summary import fromNav
from pyutil.portfolio.portfolio import Portfolio as PP
from pyutil.sql.db import Database
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.util import to_pandas, reference


class DatabaseSymbols(Database):
    def __init__(self, session=None):
        super().__init__(db="symbols", session=session)

    @property
    def nav(self):
        """
        Extract the Nav for each portfolio
        :return: frame with Nav for each portfolio (on portfolio per row)
        """
        return self._read("SELECT * FROM v_portfolio_nav", index_col="name")["data"].apply(to_pandas)

    @property
    def leverage(self):
        """
        Extract the Nav for each portfolio
        :return: frame with Nav for each portfolio (on portfolio per row)
        """
        return self._read("SELECT * FROM v_portfolio_leverage", index_col="name")["data"].apply(to_pandas)

    def sector(self, total=False):
        frame = self._read("SELECT * FROM v_portfolio_sector", index_col=["name", "symbol", "group"])["data"]
        frame = frame.apply(to_pandas).groupby(level=["name", "group"], axis=0).sum().ffill(axis=1)
        frame = frame.iloc[:,-1].unstack()

        if total:
            frame["total"] = frame.sum(axis=1)
        return frame

    def __last(self, frame, datefmt="%b %d"):
        frame = frame.sort_index(axis=1, ascending=False).rename(columns=lambda x: x.strftime(datefmt))
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    @property
    def mtd(self):
        return self.__last(self.nav.apply(lambda x: fromNav(x).mtd_series, axis=1))

    @property
    def ytd(self):
        return self.__last(self.nav.apply(lambda x: fromNav(x).ytd_series, axis=1), datefmt="%b")

    def recent(self, n=15):
        return self.__last(self.nav.apply(lambda x: fromNav(x).recent(n=n), axis=1).iloc[:, -n:])\

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
        x = self._read("SELECT * FROM v_portfolio_2 where name=%(name)s", params={"name": name},
                       index_col=["timeseries", "symbol"])["data"].apply(to_pandas)
        return PP(prices=x.loc["price"].transpose(), weights=x.loc["weight"].transpose())

    def state(self, name):
        portfolio = self.portfolio(name=name)
        ref = self._read(sql="SELECT * FROM v_symbols_state", index_col=["symbol"])

        frame = pd.concat([portfolio.state, ref.loc[portfolio.assets]], axis=1)

        sector_weights = frame.groupby(by="group")["Extrapolated"].sum()
        frame["Sector Weight"] = frame["group"].apply(lambda x: sector_weights[x])
        frame["Relative Sector"] = 100 * frame["Extrapolated"] / frame["Sector Weight"]
        frame["Asset"] = frame.index
        return frame

    @property
    def states(self):
        return {portfolio.name: self.state(name=portfolio.name) for portfolio in self.portfolios}

    @property
    def reference_symbols(self):
        return reference(self._read(sql="SELECT * FROM v_reference_symbols", index_col=["symbol", "field"]))

    def prices(self, name="PX_LAST"):
        prices = self._read(sql="SELECT * FROM v_symbols WHERE timeseries=%(NAME)s", params={"NAME": name}, index_col="name")["data"]
        return prices.apply(to_pandas).transpose()

    def symbol(self, name):
        return self.session.query(Symbol).filter_by(name=name).one()

    def strategy(self, name):
        return self.session.query(Strategy).filter_by(name=name).one()

    @property
    def strategies(self):
        for s in self.session.query(Strategy):
            yield s

    @property
    def portfolios(self):
        for p in self.session.query(Portfolio):
            yield p
