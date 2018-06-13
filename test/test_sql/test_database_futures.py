from unittest import TestCase

from pyutil.sql.base import Base
from pyutil.sql.db_futures import DatabaseFutures
from pyutil.sql.interfaces.futures.future import Future, FuturesCategory, Exchange
from pyutil.sql.model.ref import Field, DataType, FieldType
from pyutil.sql.session import postgresql_db_test
from test.config import resource


def future():
    # define an exchange
    e = Exchange(name="Chicago Mercantile Exchange")
    # define a category
    c = FuturesCategory(name="Equity Index")
    # define the future
    return Future(name="ES1 Index", internal="S&P 500 E-Mini Futures", quandl="CME/ES", exchange=e, category=c)


class TestDatabaseFutures(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = postgresql_db_test(base=Base, echo=True)

        # add views to database
        file = resource("futures.ddl")

        with open(file) as file:
            cls.session.bind.execute(file.read())

        #cls.session = session_test(meta=Base.metadata, echo=False)
        cls.f1 = Field(name="Field A", result=DataType.integer, type=FieldType.dynamic)

        cls.fut1 = future()
        cls.fut1.reference[cls.f1] = "100"

        cls.session.add_all([cls.fut1])
        cls.session.commit()

        cls.db = DatabaseFutures(session=cls.session)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()