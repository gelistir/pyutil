import pandas as pd

from pyutil.performance.summary import fromNav
from pyutil.sql.db import Database
from pyutil.sql.interfaces.symbols.frames import Frame
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.util import reference


class DatabaseSymbols(Database):
    def __init__(self, client, session=None):
        super().__init__(client=client, session=session, db="symbols")

    @property
    def nav(self):
        """
        Extract the Nav for each portfolio
        :return: frame with Nav for each portfolio (on portfolio per column)
        """
        return self.client.frame(field="nav", measurement="nav", tags=["portfolio"])

    @property
    def leverage(self):
        """
        Extract the Nav for each portfolio
        :return: frame with Nav for each portfolio (on portfolio per column)
        """
        return self.client.frame(field="leverage", measurement="leverage", tags=["portfolio"])

    def sector(self, total=False):
        def __group(symbol):
            s = self.session.query(Symbol).filter_by(name=symbol).one()
            return s.group.name

        def __sector(p):
            symbolmap = {symbol : __group(symbol) for symbol in p.symbols_influx(client=self.client)}
            return p.portfolio_influx(client=self.client).sector_weights_final(symbolmap=symbolmap, total=total)

        return pd.DataFrame({p.name : __sector(p) for p in self.session.query(Portfolio)}).transpose()

    def __last(self, frame, datefmt="%b %d"):
        frame = frame.sort_index(axis=1, ascending=False).rename(columns=lambda x: x.strftime(datefmt))
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    @property
    def mtd(self):
        return self.__last(self.nav.transpose().apply(lambda x: fromNav(x).mtd_series, axis=1))

    @property
    def ytd(self):
        return self.__last(self.nav.transpose().apply(lambda x: fromNav(x).ytd_series, axis=1), datefmt="%b")

    def recent(self, n=15):
        return self.__last(self.nav.transpose().apply(lambda x: fromNav(x).recent(n=n), axis=1).iloc[:, -n:])\

    @property
    def period_returns(self):
        return self.nav.transpose().apply(lambda x: fromNav(x).period_returns, axis=1)

    @property
    def performance(self):
        return self.nav.transpose().apply(lambda x: fromNav(x).summary(), axis=1).transpose()

    def frames(self, total=False, n=15):
        return {"recent": self.recent(n=n),
                "ytd": self.ytd,
                "mtd": self.mtd,
                "sector": self.sector(total=total),
                "periods": self.period_returns,
                "performance": self.performance}

    def portfolio(self, name: str):
        return self._filter(Portfolio, name=name).portfolio_influx(client=self.client)

    def state(self, name: str):
        portfolio = self.portfolio(name=name)
        ref = self._read(sql="SELECT * FROM v_symbols_state", index_col=["symbol"])

        frame = pd.concat([portfolio.state, ref.loc[portfolio.assets]], axis=1, sort=True)

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
        return self.client.frame(field=name, measurement="symbols", tags=["name"])

    def symbol(self, name: str):
        return self._filter(Symbol, name=name)

    def strategy(self, name: str):
        return self._filter(Strategy, name=name)

    def frame(self, name: str):
        return self._filter(Frame, name=name).frame

    @property
    def strategies(self):
        return self._iter(Strategy)

    @property
    def portfolios(self):
        return self._iter(Portfolio)

    @property
    def symbols(self):
        return self._iter(Symbol)


