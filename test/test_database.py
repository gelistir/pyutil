from unittest import TestCase

import pandas as pd

from pyutil.data import Database
from pyutil.influx.client import test_client
from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.frames import Frame
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.session import postgresql_db_test

import pandas.util.testing as pdt

class TestDatabase(TestCase):
    @classmethod
    def setUpClass(cls):
        client = test_client()
        session, connection_str = postgresql_db_test(base=Base)

        session.add(Symbol(name="Peter Maffay"))
        session.commit()

        # this will add a portfolio, too!
        session.add(Strategy(name="Peter Maffay"))
        session.commit()

        session.add(Frame(name="Peter Maffay", frame=pd.DataFrame(index=[0,1], columns=["A","B"], data=[[1,2],[3,4]])))
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

        x = self.database.symbol(name="Peter Maffay")
        self.assertEqual(x, Symbol(name="Peter Maffay"))

    def test_portfolios(self):
        p = [x for x in self.database.portfolios]
        self.assertEqual(len(p), 1)
        self.assertEqual(p[0], Portfolio(name="Peter Maffay"))

        x = self.database.portfolio(name="Peter Maffay")
        self.assertEqual(x, Portfolio(name="Peter Maffay"))

    def test_strategies(self):
        s = [x for x in self.database.strategies]
        self.assertEqual(len(s), 1)
        self.assertEqual(s[0], Strategy(name="Peter Maffay"))

        x = self.database.strategy(name="Peter Maffay")
        self.assertEqual(x, Strategy(name="Peter Maffay"))

    def test_frames(self):
        f = [x for x in self.database.frames]
        self.assertEqual(len(f), 1)
        self.assertEqual(f[0].name, "Peter Maffay")
        pdt.assert_frame_equal(f[0].frame, pd.DataFrame(index=[0,1], columns=["A","B"], data=[[1,2],[3,4]]))

        f = self.database.frame(name="Peter Maffay")
        pdt.assert_frame_equal(f.frame, pd.DataFrame(index=[0,1], columns=["A","B"], data=[[1,2],[3,4]]))
