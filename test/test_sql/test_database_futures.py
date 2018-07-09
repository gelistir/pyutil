from unittest import TestCase

from pyutil.influx.client import Client
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
        cls.session = postgresql_db_test(base=Base, echo=True, views=resource("futures.ddl"))
        cls.client = Client(host='test-influxdb', database="test-futures")

        cls.f1 = Field(name="Field A", result=DataType.integer, type=FieldType.dynamic)

        cls.fut1 = future()
        cls.fut1.reference[cls.f1] = "100"

        cls.session.add_all([cls.fut1])
        cls.session.commit()

        cls.db = DatabaseFutures(client=cls.client, session=cls.session)

    def test_future(self):
        f = self.session.query(Future).filter_by(name="ES1 Index").one()
        self.assertIsNotNone(f)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()
        cls.client.drop_database(dbname="test-futures")