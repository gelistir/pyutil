from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.influx.client import Client
from pyutil.sql.base import Base
from pyutil.sql.db_symbols import DatabaseSymbols
from pyutil.sql.interfaces.symbols.frames import Frame
from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.sql.model.ref import Field, DataType, FieldType
from pyutil.sql.session import postgresql_db_test
from test.config import test_portfolio


class TestDatabaseSymbols(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = postgresql_db_test(base=Base, echo=False)
        cls.client = Client(host='test-influxdb', database="test-AAA")
        Symbol.client = cls.client
        Portfolio.client = cls.client


        cls.f1 = Field(name="Field A", result=DataType.integer, type=FieldType.dynamic)
        cls.s1 = Symbol(name="Test Symbol", group=SymbolType.equities)
        cls.s1.ts_upsert(field="PX_LAST", ts=pd.Series({pd.Timestamp("2010-10-30"): 10.1}))

        cls.s1.reference[cls.f1] = "100"
        cls.session.add(cls.s1)
        cls.session.commit()
        cls.db = DatabaseSymbols(session=cls.session)

        cls.frame = pd.DataFrame(index=["A"], columns=["A"], data=[[1]])
        cls.frame.index.names = ["Assets"]

        cls.fr = Frame(name="Peter", frame=cls.frame)
        cls.session.add(cls.fr)
        cls.session.commit()

    def test_symbol(self):
        self.assertEqual(self.db.symbol(name="Test Symbol"), self.s1)

    def test_reference_symbols(self):
        #print(self.db.reference_symbols)

        pdt.assert_frame_equal(self.db.reference_symbols,
                               pd.DataFrame(index=["Test Symbol"], columns=["Field A"], data=[[100]]), check_names=False)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()
        cls.client.drop_database(dbname="test-AAA")


class TestPortfolio(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = postgresql_db_test(base=Base, echo=False)
        cls.client = Client(host='test-influxdb', database="test-strategy")
        Portfolio.client = cls.client

        s = Strategy(name="Peter")

        # upsert the database with the test portfolio
        s.upsert(portfolio=test_portfolio())

        # Need the assets in the database for state, etc.
        assetsA = {asset: Symbol(name=asset, group=SymbolType.equities) for asset in ["A", "B", "C"]}
        assetsB = {asset: Symbol(name=asset, group=SymbolType.fixed_income) for asset in ["D","E","F","G"]}
        assets = {**assetsA, **assetsB}

        cls.session.add_all(assets.values())

        cls.session.add(s)
        cls.session.commit()
        cls.db = DatabaseSymbols(session=cls.session)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()
        cls.client.drop_database(dbname="test-strategy")

    def test_mtd(self):
        self.assertAlmostEqual(self.db.mtd["Apr 02"]["Peter"], 0.0008949612999999967, places=5)
        self.assertAlmostEqual(self.db.ytd["Apr"]["Peter"], 0.014133604841665814, places=5)
        self.assertAlmostEqual(self.db.recent(n=20)["Apr 02"]["Peter"], 0.0008949612999999967, places=5)
        self.assertAlmostEqual(self.db.period_returns["Two weeks"]["Peter"], 0.008663804539365882, places=5)

    def test_sector(self):
        pdt.assert_series_equal(self.db.sector().loc["Peter"], pd.Series(index=["equities","fixed_income"], data=[0.135671, 0.173303]), check_names=False)
        pdt.assert_series_equal(self.db.sector(total=True).loc["Peter"],
                                pd.Series(index=["equities", "fixed_income", "total"], data=[0.135671, 0.173303, 0.3089738755]),
                                check_names=False)


    def test_frames(self):
        x = self.db.frames()
        pdt.assert_frame_equal(x["performance"], self.db.performance)
        pdt.assert_frame_equal(x["recent"], self.db.recent())
        pdt.assert_frame_equal(x["sector"], self.db.sector())
        pdt.assert_frame_equal(x["mtd"], self.db.mtd)
        pdt.assert_frame_equal(x["ytd"], self.db.ytd)
        pdt.assert_frame_equal(x["periods"], self.db.period_returns)

        self.assertEqual(len(x), 6)


    def test_reference_symbols(self):
        self.assertTrue(self.db.reference_symbols.empty)

