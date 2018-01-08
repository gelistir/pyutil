from unittest import TestCase

import pandas as pd


from pyutil.sql.db import db, Timeseries, Symbol, TimeseriesData, SymbolGroup, Type, Field, SymbolReference, upsert_frame, Frame, upsert_portfolio, PortfolioSQL, asset, Strategy
from test.config import read_frame, test_portfolio, resource
from pyutil.sql.pony import db_in_memory

import pandas.util.testing as pdt


class TestHistory(TestCase):
    def test_series(self):
        with db_in_memory(db):
            prices = read_frame("price.csv")
            for symbol in ["A", "B", "C", "D"]:
                ts = Timeseries(symbol=Symbol(bloomberg_symbol=symbol, group=SymbolGroup(name=symbol)), name="PX_LAST")
                for date, value in prices[symbol].dropna().items():
                    TimeseriesData(ts=ts, date=date, value=value)

            t = Symbol.get(bloomberg_symbol="A").series["PX_LAST"]
            pdt.assert_series_equal(t, read_frame("price.csv")["A"].dropna(), check_names=False)

            tt = Timeseries.get(symbol=Symbol.get(bloomberg_symbol="A"), name="PX_LAST")
            self.assertFalse(tt.empty)
            self.assertEqual(tt.last_valid, pd.Timestamp("2015-04-22").date())


    def test_ref(self):
        with db_in_memory(db):
            t1 = Type(name="BB-static", comment="Static data Bloomberg")
            t2 = Type(name="BB-dynamic", comment="Dynamic data Bloomberg")
            t3 = Type(name="user-defined", comment="User-defined reference data")
            Field(name="CHG_PCT_1D", type=t2)
            Field(name="NAME", type=t1)
            Field(name="REGION", type=t3)

            Symbol(bloomberg_symbol="XX", group=SymbolGroup(name="A"))
            Symbol(bloomberg_symbol="YY", group=SymbolGroup(name="B"))

            SymbolReference(field=Field.get(name="CHG_PCT_1D"), symbol=Symbol.get(bloomberg_symbol="XX"), content="0.40")
            SymbolReference(field=Field.get(name="NAME"), symbol=Symbol.get(bloomberg_symbol="XX"), content="Hans")
            SymbolReference(field=Field.get(name="REGION"), symbol=Symbol.get(bloomberg_symbol="XX"), content="Europe")

            SymbolReference(field=Field.get(name="CHG_PCT_1D"), symbol=Symbol.get(bloomberg_symbol="YY"), content="0.40")
            SymbolReference(field=Field.get(name="NAME"), symbol=Symbol.get(bloomberg_symbol="YY"), content="Urs")
            SymbolReference(field=Field.get(name="REGION"), symbol=Symbol.get(bloomberg_symbol="YY"), content="Europe")


            s = Symbol.get(bloomberg_symbol="XX")
            self.assertEqual(s.reference["REGION"], "Europe")

    def test_frame(self):
        with db_in_memory(db):
            x = pd.DataFrame(data=[[1.2, 1.0], [1.0, 2.1]], index=["A","B"], columns=["X1", "X2"])
            x.index.names = ["index"]

            upsert_frame(name="test", frame=x)

            f = Frame.get(name="test").frame

            pdt.assert_frame_equal(f, x)


    def test_portfolio(self):
        with db_in_memory(db):
            p = test_portfolio()
            SymbolGroup(name="A")
            SymbolGroup(name="B")

            for symbol in ["A", "B", "C", "D"]:
                Symbol(bloomberg_symbol=symbol, group=SymbolGroup.get(name="A"))

            for symbol in ["E", "F", "G"]:
                Symbol(bloomberg_symbol=symbol, group=SymbolGroup.get(name="B"))

            upsert_portfolio(name="test", portfolio=p)

            x = PortfolioSQL.get(name="test")

            pdt.assert_frame_equal(x.portfolio.weights, p.weights)
            pdt.assert_frame_equal(x.portfolio.prices, p.prices)
            pdt.assert_series_equal(x.nav, p.nav)
            self.assertAlmostEquals(x.sector["A"]["2013-01-25"],0.1069106628 )
            print(x.sector)



    def test_asset(self):
        with db_in_memory(db):
            Symbol(bloomberg_symbol="XX", group=SymbolGroup(name="A"))
            assert asset(name="XX")


    def test_strategy(self):
        with db_in_memory(db):
            s = Strategy(name="test", source="No", active=True)
            self.assertIsNone(s.last_valid)
            self.assertIsNone(s.portfolio)
            self.assertIsNone(s.assets)

            s.upsert_portfolio(portfolio=test_portfolio())
            self.assertIsNotNone(s.portfolio)

            pdt.assert_frame_equal(s.portfolio.weight, test_portfolio().weights)
            pdt.assert_frame_equal(s.portfolio.price, test_portfolio().prices)

            self.assertEquals(s.last_valid, pd.Timestamp("2015-04-22"))

            # upsert for a second time....
            s.upsert_portfolio(portfolio=test_portfolio())
            self.assertIsNotNone(s.portfolio)
            pdt.assert_frame_equal(s.portfolio.weight, test_portfolio().weights)
            pdt.assert_frame_equal(s.portfolio.price, test_portfolio().prices)
            self.assertEquals(s.last_valid, pd.Timestamp("2015-04-22"))

    def test_strategy_code(self):
        with db_in_memory(db):
            f = open(resource("source.py"),"r")

            s = Strategy(name="test", source=f.read(), active=True)
            s.upsert_portfolio(s.compute_portfolio(reader=asset))
            self.assertIsNotNone(s.portfolio)

            pdt.assert_frame_equal(s.portfolio.weight, test_portfolio().weights)
            pdt.assert_frame_equal(s.portfolio.price, test_portfolio().prices)

            self.assertEquals(s.last_valid, pd.Timestamp("2015-04-22"))
            self.assertEquals(s.assets, test_portfolio().assets)

    def test_timeseries(self):
        with db_in_memory(db):
            ts = Timeseries(name="PX_LAST", symbol=Symbol(bloomberg_symbol="A"))
            self.assertIsNone(ts.last_valid)
            self.assertTrue(ts.empty)
            ts.upsert(ts=read_frame("price.csv")["A"])
            pdt.assert_series_equal(ts.series, read_frame("price.csv")["A"], check_names=False)
            self.assertEquals(ts.last_valid, pd.Timestamp("2015-04-22").date())

