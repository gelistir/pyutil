import pandas as pd

from pyutil.performance.summary import fromNav
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.model.ref import Field


class Database(object):

    def __init__(self, session):
        # session, sql database
        self.__session = session

    def close(self):
        self.__session.close()

    @staticmethod
    def __last(frame, datefmt="%b %d"):
        frame = frame.sort_index(axis=1, ascending=False).rename(columns=lambda x: x.strftime(datefmt))
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame #return Database.__reindex(frame)

    #@staticmethod
    #def __reindex(frame):
    #    frame.index = [a.name for a in frame.index]
    #    return frame

    @staticmethod
    def __percentage(x):
        return "{0:.2f}%".format(float(100.0 * x)).replace("nan%", "")

    def recent(self, n=15):
        # define the function
        g = self.nav(f=lambda x: fromNav(x).recent(n)).tail(n).transpose()
        return self.__last(g).applymap(self.__percentage)

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

    @property
    def reference(self):
        return Symbol.reference_frame(self.symbols)

    def sector(self, total=False):
        frame = pd.DataFrame({p.name: p.sector(total=total).iloc[-1] for p in self.portfolios}).transpose()
        return frame

    def nav(self, f=None):
        f = f or (lambda x: x)

        # we prefer this solution as is goes through the cleaner SQL database!
        return pd.DataFrame({portfolio.name: f(portfolio.nav) for portfolio in self.portfolios})

    def history(self, field):
        return pd.DataFrame({symbol.name: symbol.ts[field] for symbol in self.symbols})

    def mtd(self):
        frame = self.nav(f=lambda x: fromNav(x).returns)
        today = frame.index[-1]

        if today.day < 5:
            first_day_of_month = (today + pd.offsets.MonthBegin(-2)).date()
        else:
            first_day_of_month = (today + pd.offsets.MonthBegin(-1)).date()

        frame = frame.truncate(before=first_day_of_month)
        return self.__last(frame.transpose()).applymap(self.__percentage)

    def ytd(self):
        g = self.nav(f=lambda x: fromNav(x).ytd_series).transpose()
        g["total"] = (g + 1).prod(axis=1) - 1
        #g = self.__reindex(g)
        return g.applymap(self.__percentage)

    def nav_asset(self, name, f=lambda x: x, **kwargs):
        symbol = self.symbol(name=name)
        nav = fromNav(symbol.ts["PX_LAST"])
        vola = nav.ewm_volatility()
        drawdown = nav.drawdown

        return {**{"nav": f(nav), "drawdown": f(drawdown), "volatility": f(vola)}, **kwargs}

    def nav_strategy(self, name,  f=lambda x: x, **kwargs):
        portfolio = self.portfolio(name=name)
        nav = portfolio.nav
        vola = nav.ewm_volatility()
        drawdown = nav.drawdown

        return {**{"nav": f(nav), "drawdown": f(drawdown), "volatility": f(vola)}, **kwargs}

        #return math_dictionary(portfolio.nav, name=name)


        return math_dictionary(symbol.ts["PX_LAST"], name=name)

    def nav_strategy(self, name):
        portfolio = self.portfolio(name=name)
        return math_dictionary(portfolio.nav, name=name)


