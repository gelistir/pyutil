import pandas as pd

from pyutil.performance.summary import fromNav
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.symbol import Symbol


class DatabaseSymbols(object):
    def __init__(self, session):
        self.session = session

    @property
    def nav(self):
        """
        Extract the Nav for each portfolio
        :return: frame with Nav for each portfolio (on portfolio per column)
        """
        return Portfolio.nav_all()

    def sector(self, total=False):
        return pd.DataFrame({p.name: p.sector(total=total).iloc[-1] for p in self.session.query(Portfolio)}).transpose()

    def __last(self, frame, datefmt="%b %d"):
        frame = frame.sort_index(axis=1, ascending=False).rename(columns=lambda x: x.strftime(datefmt))
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    def __trans_nav(self, f):
        return Portfolio.nav_all().apply(f).transpose()

    @property
    def mtd(self):
        return self.__last(self.__trans_nav(lambda x: fromNav(x).mtd_series))

    @property
    def ytd(self):
        return self.__last(self.__trans_nav(lambda x: fromNav(x).ytd_series), datefmt="%b")

    def recent(self, n=15):
        return self.__last(self.__trans_nav(lambda x: fromNav(x).recent(n=n)).iloc[:, -n:])

    @property
    def period_returns(self):
        return self.__trans_nav(lambda x: fromNav(x).period_returns)

    @property
    def performance(self):
        return self.__trans_nav(lambda x: fromNav(x).summary()).transpose()

    def frames(self, total=False, n=15):
        return {"recent": self.recent(n=n),
                "ytd": self.ytd,
                "mtd": self.mtd,
                "sector": self.sector(total=total),
                "periods": self.period_returns,
                "performance": self.performance}

    @property
    def reference_symbols(self):
        # reference frame for all symbols
        return Symbol.reference_frame(self.session.query(Symbol))

    def symbol(self, name: str):
        return self.session.query(Symbol).filter_by(name=name).one()
