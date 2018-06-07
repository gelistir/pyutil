from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.base import Base
from pyutil.sql.db_symbols import Database
from pyutil.sql.interfaces.products import Products
from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.sql.model.ref import Field, DataType, FieldType
from pyutil.sql.session import test_postgresql_db
from test.config import resource, test_portfolio


class TestDatabaseSymbols(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = test_postgresql_db(echo=True)

        Base.metadata.create_all(cls.session.bind)

        # add views to database
        file = resource("symbols.ddl")

        with open(file) as file:
            cls.session.bind.execute(file.read())

        cls.f1 = Field(name="Field A", result=DataType.integer, type=FieldType.dynamic)
        cls.s1 = Symbol(name="Test Symbol", group=SymbolType.equities)
        cls.s1.upsert_ts(name="PX_LAST", data={pd.Timestamp("2010-10-30").date(): 10.1})

        cls.s1.reference[cls.f1] = "100"
        cls.session.add(cls.s1)
        cls.session.commit()
        cls.db = Database(session=cls.session)


    def test_symbol(self):
        self.assertEqual(self.db.symbol(name="Test Symbol"), self.s1)

    def test_reference_symbols(self):
        pdt.assert_frame_equal(self.db.reference_symbols, pd.DataFrame(index=["Test Symbol"], columns=["Field A"], data=[[100]]), check_names=False)

    def test_prices_symbols(self):
        pdt.assert_frame_equal(self.db.prices(), pd.DataFrame(index=[pd.Timestamp("2010-10-30")], columns=["Test Symbol"], data=[[10.1]]), check_names=False)

    @classmethod
    def tearDownClass(cls):
        cls.session.close()


class TestPortfolio(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = test_postgresql_db(echo=False)

        Base.metadata.create_all(cls.session.bind)

        # add views to database
        file = resource("symbols.ddl")

        with open(file) as file:
            cls.session.bind.execute(file.read())

        with open(resource("source.py"), "r") as f:
            s = Strategy(name="Peter", source=f.read(), active=True)

        # this will just return the test_portfolio
        config = s.configuration(reader=None)
        portfolio = config.portfolio

        # This will return the assets as given by the reader!
        assetsA = {asset: Symbol(name=asset, group=SymbolType.equities) for asset in ["A", "B", "C"]}
        assetsB = {asset: Symbol(name=asset, group=SymbolType.fixed_income) for asset in ["D","E","F","G"]}
        assets = {**assetsA, **assetsB}

        #for name, asset in assets:
        #    assets.reference[]
        cls.session.add_all(assets.values())

        # store the portfolio we have just computed in there...
        s.upsert(portfolio, assets=assets)

        cls.session.add(s)
        cls.session.commit()
        cls.db = Database(session=cls.session)

    def test_mtd(self):
        self.assertAlmostEqual(self.db.mtd["Apr 02"]["Peter"], 0.0008949612999999967, places=5)
        self.assertAlmostEqual(self.db.ytd["Apr"]["Peter"], 0.014133604841665814, places=5)
        self.assertAlmostEqual(self.db.recent(n=20)["Apr 02"]["Peter"], 0.0008949612999999967, places=5)
        self.assertAlmostEqual(self.db.period_returns["Two weeks"]["Peter"], 0.008663804539365882, places=5)

    def test_portfolio(self):
        x = self.db.portfolio(name="Peter")
        columns = x.prices.keys()
        pdt.assert_frame_equal(x.prices[columns], test_portfolio().prices[columns], check_names=False)
        pdt.assert_frame_equal(x.weights[columns], test_portfolio().weights[columns], check_names=False)

    def test_nav(self):
        pdt.assert_series_equal(test_portfolio().nav, self.db.nav.loc["Peter"], check_names=False)
        self.assertAlmostEqual(self.db.performance["Peter"]["Calmar Ratio (3Y)"], 0.07615829203518315, places=5)

    def test_sector(self):
        pdt.assert_series_equal(self.db.sector().loc["Peter"][pd.Timestamp("2015-04-22")], pd.Series(index=["equities","fixed_income"], data=[0.135671, 0.173303]), check_names=False)

    def test_frames(self):
        x = self.db.frames()
        pdt.assert_frame_equal(x["performance"], self.db.performance)
        pdt.assert_frame_equal(x["recent"], self.db.recent())
        pdt.assert_frame_equal(x["sector"], self.db.sector())
        pdt.assert_frame_equal(x["mtd"], self.db.mtd)
        pdt.assert_frame_equal(x["ytd"], self.db.ytd)
        pdt.assert_frame_equal(x["periods"], self.db.period_returns)

        self.assertEqual(len(x), 6)

    def test_state(self):
        print(self.db.state(name="Peter"))

    def test_products(self):
        p=Products(session=self.session)
        print(p.products("symbol"))
        a = p.reference("symbol")
        self.assertListEqual(["product", "field"], a.index.names)
        self.assertTrue(a.empty)
        self.assertIsInstance(a, pd.DataFrame)



        #, type="symbol")
        #print(p.x)
        #print(p.reference())
        #assert False
