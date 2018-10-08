from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.symbol import Symbol


def engine(sql, echo=False):
    """ Create a fresh new session... """
    return create_engine(sql, echo=echo)


#def influx_client(self):
#    # here you read from the environment variables!
#    return Client()


def connection(sql, echo=False):
    return engine(sql=sql, echo=echo).connect()


def session(sql, echo=False):
    return Session(bind=connection(sql=sql, echo=echo))


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
    def influx_client(self):
        return self.__client

    @property
    def symbols(self):
        return self.__session.query(Symbol)

    @property
    def portfolios(self):
        return self.__session.query(Portfolio)

    @contextmanager
    def session(self, echo=False):
        """Provide a transactional scope around a series of operations."""
        try:
            s = self.__session
            yield s
            s.commit()
        except Exception as e:
            s.rollback()
            raise e
        finally:
            s.close()
