from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.models import Frame, Symbol, Field, Strategy, FieldType, \
    SymbolType, PortfolioSQL, _Timeseries, DataType
from test.config import test_portfolio, resource, read_frame


class TestModels(TestCase):

    def test_field_1(self):
        f = Field(name="Field 1", type=FieldType.dynamic, resulttype=DataType.string)

        self.assertEqual(str(f), "Field 1")
        self.assertEqual(f.type, FieldType.dynamic)
        self.assertEqual(f.resulttype, DataType.string)

        self.assertEqual(f, Field(name="Field 1", type=FieldType.dynamic))
        s = Symbol(bloomberg_symbol="A")
        f.refdata[s] = "AHA"
        self.assertEqual(f.refdata[s], "AHA")
        pdt.assert_series_equal(f.reference, pd.Series({"A": "AHA"}))

    def test_field_2(self):
        f = Field(name="Field 1", type=FieldType.dynamic, resulttype=DataType.date)

        self.assertEqual(str(f), "Field 1")
        self.assertEqual(f.type, FieldType.dynamic)
        self.assertEqual(f.resulttype, DataType.date)

        self.assertEqual(f, Field(name="Field 1", type=FieldType.dynamic))
        s = Symbol(bloomberg_symbol="A")
        f.refdata[s] = "12-11-1978"
        self.assertEqual(f.refdata[s], pd.Timestamp("12-11-1978").date())
        pdt.assert_series_equal(f.reference, pd.Series({"A": pd.Timestamp("12-11-1978").date()}))

    def test_symbol(self):
        s = Symbol(bloomberg_symbol="Symbol 1", group=SymbolType.equities, internal="Symbol 1 internal")

        self.assertEqual(s.bloomberg_symbol, "Symbol 1")
        self.assertEqual(str(s), "Symbol 1")

        f = Field(name="Field 1", type=FieldType.dynamic, resulttype=DataType.integer)
        s.refdata[f] = 100

        self.assertEqual(s.refdata[f], 100)
        self.assertSetEqual(set(s.refdata.keys()), {f})

        pdt.assert_series_equal(s.reference, pd.Series({"Field 1": 100}))

        # update the already existing field
        s.refdata[f] = 200

        pdt.assert_series_equal(s.reference, pd.Series({"Field 1": 200}))

    def test_timeseries(self):
        x = _Timeseries(name="Peter")
        self.assertTrue(x.series.empty)
        x.upsert(ts=pd.Series({1: 2.0, 5: 3.0}))
        pdt.assert_series_equal(x.series, pd.Series({1: 2.0, 5: 3.0}))

    def test_timeseries_of_symbol(self):
        s = Symbol(bloomberg_symbol="Symbol 1", group=SymbolType.equities, internal="Symbol 1 internal")
        s.timeseries["Peter"] = pd.Series({1: 2.0, 5: 3.0})
        # this will return a pandas series
        t1 = s.timeseries["Peter"]
        # this will return the actual Timeseries class
        t2 = s._timeseries["Peter"]


        self.assertFalse(t1.empty)
        self.assertEqual(t1.last_valid_index(), 5)

        t2.upsert(ts=pd.Series({1: 7.0, 6: 3.0}))

        t1 = s.timeseries["Peter"]
        self.assertFalse(t1.empty)
        self.assertEqual(t1.last_valid_index(), 6)

        pdt.assert_series_equal(t1, pd.Series({1: 7.0, 5: 3.0, 6: 3.0}))

    def test_frame(self):
        x = pd.DataFrame(data=[[1.2, 1.0], [1.0, 2.1]], index=["A", "B"], columns=["X1", "X2"])
        x.index.names = ["index"]
        f = Frame(frame=x, name="test")
        pdt.assert_frame_equal(f.frame, x)

    def test_strategy(self):
        with open(resource("source.py"), "r") as f:
            s = Strategy(name="peter", source=f.read(), active=True)
            self.assertListEqual(s.assets, [])
            self.assertTrue(s.portfolio.empty)
            self.assertIsNone(s._portfolio.last_valid)

            s.portfolio = s.compute_portfolio(reader=None)

            self.assertIsNotNone(s._portfolio)
            self.assertIsNotNone(s.portfolio)

            pdt.assert_frame_equal(s._portfolio.weight, test_portfolio().weights)
            pdt.assert_frame_equal(s._portfolio.price, test_portfolio().prices)

            self.assertEqual(s._portfolio.last_valid, pd.Timestamp("2015-04-22"))
            self.assertListEqual(s.assets, ['A', 'B', 'C', 'D', 'E', 'F', 'G'])

            # upsert an existing portfolio

            p = test_portfolio().tail(10)
            # double the weights for the last 5 days of the existing portfolio
            x = s.upsert(portfolio=2 * p, days=5)
            # check for the jump in leverage
            self.assertAlmostEqual(x.leverage.loc["2015-04-16"], 0.3121538556, places=7)
            self.assertAlmostEqual(x.leverage.loc["2015-04-17"], 0.6213015319, places=7)

            s = Strategy(name="maffay")
            x=s.upsert(portfolio=test_portfolio())
            self.assertAlmostEqual(x.leverage.loc["2015-04-16"], 0.3121538556, places=7)
            self.assertAlmostEqual(x.leverage.loc["2015-04-17"], 0.3106507659, places=7)

    def test_portfolio(self):
        portfolio = test_portfolio()

        p = PortfolioSQL(name="test")
        self.assertTrue(p.empty)
        p.upsert(portfolio=portfolio)
        self.assertFalse(p.empty)

        pdt.assert_frame_equal(p.price, test_portfolio().prices)
        pdt.assert_frame_equal(p.weight, test_portfolio().weights)
        self.assertEqual(p.assets, test_portfolio().assets)
        self.assertEqual(p.last_valid, test_portfolio().index[-1])

        self.assertAlmostEqual(p.nav.sharpe_ratio(), 0.1127990962306739, places=10)

        # test the truncation
        p1 = portfolio.truncate(after=pd.Timestamp("2015-01-01") - pd.DateOffset(seconds=1))
        pp = PortfolioSQL(name="wurst")

        pp.upsert(portfolio=test_portfolio().truncate(after=pd.Timestamp("2015-02-01")))
        self.assertEqual(p1.index[-1], pd.Timestamp("2014-12-31"))
        p2 = portfolio.truncate(before=pd.Timestamp("2015-01-01").date())

        pp.upsert(portfolio=p2)
        pdt.assert_frame_equal(pp.weight, test_portfolio().weights)

    def test_portfolio_sector(self):
        portfolio = test_portfolio()
        p = PortfolioSQL(name="test")
        self.assertIsNone(p.last_valid)

        p.upsert(portfolio=portfolio)
        symbolmap=pd.Series({"A": "A", "B": "A", "C": "B", "D": "B", "E": "C", "F": "C", "G": "C"})
        x = p.sector(map=symbolmap, total=True)
        pdt.assert_frame_equal(x.head(10), read_frame("sector_weights.csv", parse_dates=True))

