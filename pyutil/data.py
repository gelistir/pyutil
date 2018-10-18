import pandas as pd

from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.model.ref import Field


class Database(object):

    def __init__(self, session, client=None):
        # session, sql database
        self.__session = session
        #if client:
        #    self.__client = client
        #    # this is how the sql database is using influx...
        #    ProductInterface.client = self.__client

    def close(self):
        self.__session.close()
        #if self.__client:
        #    self.__client.close()

    #@property
    #def influx_client(self):
    #    return self.__client

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
    def frames(self):
        return self.__session.query(Frame)

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
        return pd.DataFrame({p: p.sector(total=total).iloc[-1] for p in self.portfolios}).transpose()

    def nav(self, f=None):
        f = f or (lambda x: x)

        # we prefer this solution as is goes through the cleaner SQL database!
        return pd.DataFrame({portfolio: f(portfolio.nav) for portfolio in self.portfolios})
