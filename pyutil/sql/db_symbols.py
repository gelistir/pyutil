from pyutil.sql.interfaces.symbols.portfolio import Portfolio, Portfolios
from pyutil.sql.interfaces.symbols.symbol import Symbol, Symbols
from pyutil.sql.session import session as sss


class Database(object):
    def __init__(self, session=None):
        self.__session = session or sss()

    @property
    def symbols(self):
        return Symbols(self.__session.query(Symbol))

    @property
    def portfolios(self):
        return Portfolios(self.__session.query(Portfolio))

    def symbol(self, bloomberg_symbol):
        return self.__session.query(Symbol).filter_by(bloomberg_symbol = bloomberg_symbol).one()

    def portfolio(self, name):
        return self.__session.query(Portfolio).filter_by(name = name).one()
