import pandas as pd

from pyutil.performance.summary import fromNav
from pyutil.sql.db import Database
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.symbol import Symbol


class DatabaseSymbols(Database):
    def __init__(self, client, session=None):
        super().__init__(client=client, session=session, db="symbols")

    @property
    def nav(self):
        """
        Extract the Nav for each portfolio
        :return: frame with Nav for each portfolio (on portfolio per column)
        """
        return Portfolio.nav_all(client=self.client)

    def sector(self, total=False):
        def __sector(p):
            portfolio = p.portfolio_influx(client=self.client)
            symbolmap = {symbol : self.symbol(name=symbol).group.name for symbol in portfolio.assets}
            return portfolio.sector_weights_final(symbolmap=symbolmap, total=total)

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

    @property
    def reference_symbols(self):
        return Symbol.reference_frame(self.session.query(Symbol))

    #def prices(self, name="PX_LAST"):
    #    return Symbol.read_frame(client=self.client, field=name)

    def symbol(self, name: str):
        return self._filter(Symbol, name=name)
