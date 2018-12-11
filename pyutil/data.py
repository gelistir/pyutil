import pandas as pd
import numpy as np

from pyutil.performance.summary import fromNav
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.model.ref import Field


class Database(object):
    @staticmethod
    def _f(series):
        if series is not None and not series.empty:
            return series
        else:
            return np.nan

    def __init__(self, session):
        # session, sql database
        self.__session = session

    def close(self):
        self.__session.close()

    @staticmethod
    def __last(frame, datefmt=None):
        frame = frame.sort_index(axis=1, ascending=False)
        if datefmt:
            frame = frame.rename(columns=lambda x: x.strftime(datefmt))
        frame["total"] = (frame + 1).prod(axis=1) - 1
        frame.index.name = "Portfolio"
        return frame

    @staticmethod
    def __percentage(x):
        return "{0:.2f}%".format(float(100.0 * x)).replace("nan%", "")

    def recent(self, n=15):
        # define the function
        g = self.nav(f=lambda x: fromNav(x).recent(n)).tail(n).transpose()
        return self.__last(g, datefmt="%b %d").applymap(self.__percentage)

    @property
    def session(self):
        return self.__session

    @property
    def symbols(self):
        return self.__session.query(Symbol)

    @property
    def portfolios(self):
        return self.__session.query(Portfolio)

    @property
    def strategies(self):
        return self.__session.query(Strategy)

    @property
    def fields(self):
        return self.__session.query(Field)

    def symbol(self, name):
        return self.symbols.filter(Symbol.name == name).one()

    def portfolio(self, name):
        return self.portfolios.filter(Portfolio.name == name).one()

    def strategy(self, name):
        return self.strategies.filter(Strategy.name == name).one()

    def reference(self, symbols=None):
        symbols = symbols or self.symbols
        return Symbol.reference_frame(symbols, name="Symbol")

    def sector(self, total=False):
        frame = pd.DataFrame({strategy.name: strategy.sector(total=total).iloc[-1] for strategy in self.strategies}).transpose()
        frame.index.name = "Portfolio"
        return frame

    def nav(self, f=lambda x: x):
        return pd.DataFrame({strategy.name: f(strategy.portfolio.nav) for strategy in self.strategies})

    def history(self, symbols=None):
        symbols = symbols or self.symbols
        return pd.DataFrame({symbol.name: Database._f(symbol.price) for symbol in symbols})

    def mtd(self):
        frame = self.nav(f=lambda x: fromNav(x).returns)
        today = frame.index[-1]

        if today.day < 5:
            first_day_of_month = (today + pd.offsets.MonthBegin(-2)).date()
        else:
            first_day_of_month = (today + pd.offsets.MonthBegin(-1)).date()

        frame = frame.truncate(before=first_day_of_month)
        return self.__last(frame.transpose(), datefmt="%b %d").applymap(self.__percentage)

    def ytd(self):
        g = self.nav(f=lambda x: fromNav(x).ytd_series).transpose()
        return self.__last(g).applymap(self.__percentage)
