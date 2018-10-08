from unittest import TestCase

from pyutil.data import Database
from pyutil.influx.client import test_client
from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.session import postgresql_db_test


class TestDatabase(TestCase):
    @classmethod
    def setUpClass(cls):
        client = test_client()
        session, connection_str = postgresql_db_test(base=Base)

        session.add(Symbol(name="Peter Maffay"))
        session.commit()

        session.add(Portfolio(name="Peter Maffay"))
        session.commit()

        cls.database = Database(client=client, session=session)

    @classmethod
    def tearDownClass(cls):
        cls.database.close()

    def test_client(self):
        self.assertIsNotNone(self.database.influx_client)

    def test_session(self):
        self.assertIsNotNone(self.database.session)

    def test_symbols(self):
        s = [x for x in self.database.symbols]
        self.assertEqual(len(s), 1)
        self.assertEqual(s[0], Symbol(name="Peter Maffay"))

    def test_portfolios(self):
        p = [x for x in self.database.portfolios]
        self.assertEqual(len(p), 1)
        self.assertEqual(p[0], Portfolio(name="Peter Maffay"))

