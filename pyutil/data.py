from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.symbol import Symbol


class Database(object):

    def __init__(self, session, client):
        # session, sql database
        self.__session = session
        self.__client = client
        # this is how the sql database is using influx...
        ProductInterface.client = self.__client

    def close(self):
        self.__session.close()
        self.__client.close()

    @property
    def session(self):
        return self.__session

    @property
    def influx_client(self):
        return self.__client

    @property
    def symbols(self):
        return self.__session.query(Symbol)

    @property
    def portfolios(self):
        return self.__session.query(Portfolio)