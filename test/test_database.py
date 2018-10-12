from unittest import TestCase

import pandas as pd

from pyutil.data import Database
from pyutil.influx.client import test_client
from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.frames import Frame
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.sql.session import postgresql_db_test

import pandas.util.testing as pdt

from test.config import test_portfolio


class TestDatabase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.symbols = dict()
        for name in ["A", "B", "C"]:
            cls.symbols[name] = Symbol(name=name, group=SymbolType.equities)

        for name in ["D","E","F","G"]:
            cls.symbols[name] = Symbol(name=name, group=SymbolType.fixed_income)

        client = test_client()
        client.recreate(dbname="test")

        session, connection_str = postgresql_db_test(base=Base)
        cls.database = Database(client=client, session=session)

        # this will add a portfolio, too!
        s = Strategy(name="Peter Maffay")
        s.upsert(portfolio=test_portfolio(), symbols=cls.symbols)

        session.add(s)
        session.commit()

        session.add(Frame(name="Peter Maffay", frame=test_portfolio().prices))
        session.commit()


    @classmethod
    def tearDownClass(cls):
        cls.database.close()

    def test_client(self):
        self.assertIsNotNone(self.database.influx_client)

    def test_session(self):
        self.assertIsNotNone(self.database.session)

    def test_symbols(self):
        self.assertEqual(self.database.symbols.count(), 7)
        self.assertEqual(self.database.symbol(name="A"), Symbol(name="A"))

    def test_portfolios(self):
        self.assertEqual(self.database.portfolios.count(), 1)
        self.assertEqual(self.database.portfolio(name="Peter Maffay"), Portfolio(name="Peter Maffay"))

    def test_strategies(self):
        self.assertEqual(self.database.strategies.count(), 1)
        self.assertEqual(self.database.strategy(name="Peter Maffay"), Strategy(name="Peter Maffay"))

    def test_frames(self):
        self.assertEqual(self.database.frames.count(), 1)
        self.assertEqual(self.database.frame(name="Peter Maffay").name, "Peter Maffay")

        pdt.assert_frame_equal(self.database.frame(name="Peter Maffay").frame,
                               test_portfolio().prices)

    def test_nav(self):
        f = self.database.nav()
        p = self.database.portfolio(name="Peter Maffay")
        pdt.assert_series_equal(f[p], pd.Series(test_portfolio().nav), check_names=False)

    def test_sector(self):
        f = self.database.sector(total=False)
        p = self.database.portfolio(name="Peter Maffay")
        frame = pd.DataFrame(index=[p], columns=["equities", "fixed_income"], data=[[0.135671, 0.173303]])
        pdt.assert_frame_equal(f, frame)



