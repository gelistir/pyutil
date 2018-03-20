from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.models import Frame, Symbol, Field, Strategy, FieldType, \
    SymbolType, Timeseries, PortfolioSQL, _SymbolReference
from test.config import test_portfolio, resource


class TestModels(TestCase):

    def test_field(self):
        f = Field(name="Field 1", type=FieldType.dynamic)
        self.assertEqual(str(f), "Field 1")

        #print(dir(f))
        #self.assertDictEqual(f.f, {})
        self.assertEqual(f.type, FieldType.dynamic)
        self.assertEqual(f, Field(name="Field 1", type=FieldType.dynamic))

    def test_symbol(self):
        s = Symbol(bloomberg_symbol="Symbol 1", group=SymbolType.equities, internal="Symbol 1 internal")
        self.assertTrue(s.reference.empty)

        #self.assertEqual(s.fff, {})
        self.assertEqual(s.bloomberg_symbol, "Symbol 1")
        self.assertEqual(str(s), "Symbol 1")
        self.assertDictEqual(s.timeseries, {})
        self.assertTrue(s.reference.empty)

        f = Field(name="Field 1", type=FieldType.dynamic)

        a = s.update_reference(f, "100")
        # this will only work after commit?
        self.assertEqual(a.field, f)
        self.assertEqual(a.symbol, s)
        self.assertEqual(a.content, "100")

        self.assertDictEqual(s._fields, {"Field 1": a})

        pdt.assert_series_equal(f.reference, pd.Series({"Symbol 1": "100"}))
        pdt.assert_series_equal(s.reference, pd.Series({"Field 1": "100"}))

        # update the already existing field
        a = s.update_reference(f, "200")

        self.assertEqual(a.field, f)
        self.assertEqual(a.symbol, s)
        self.assertEqual(a.content, "200")

        self.assertDictEqual(s._fields, {"Field 1": a})
        self.assertDictEqual(f._symbols, {"Symbol 1": a})

        pdt.assert_series_equal(s.reference, pd.Series({"Field 1": "200"}))
        pdt.assert_series_equal(f.reference, pd.Series({"Symbol 1": "200"}))

    def test_timeseries(self):
        s = Symbol(bloomberg_symbol="Symbol 1", group=SymbolType.equities, internal="Symbol 1 internal")
        t = Timeseries(name="Peter", symbol=s)

        self.assertTrue(t.empty)
        self.assertIsNone(t.last_valid)
        pdt.assert_series_equal(t.series, pd.Series({}))

        t.upsert(ts=pd.Series({1: 2.0, 5: 3.0}))

        self.assertFalse(t.empty)
        self.assertEqual(t.last_valid, 5)

        self.assertEqual(str(t), "Peter for Symbol 1")

        # upsert an already existing key

        t.upsert(ts=pd.Series({1: 7.0, 6: 3.0}))

        self.assertFalse(t.empty)
        self.assertEqual(t.last_valid, 6)

        pdt.assert_series_equal(t.series, pd.Series({1: 7.0, 5: 3.0, 6: 3.0}))

    def test_frame(self):
        x = pd.DataFrame(data=[[1.2, 1.0], [1.0, 2.1]], index=["A", "B"], columns=["X1", "X2"])
        x.index.names = ["index"]
        f = Frame(frame=x, name="test")
        pdt.assert_frame_equal(f.frame, x)

    def test_strategy(self):
        with open(resource("source.py"), "r") as f:
            s = Strategy(name="peter", source=f.read(), active=True)
            self.assertListEqual(s.assets, [])

            s.upsert(portfolio=s.compute_portfolio(reader=None))
            self.assertIsNotNone(s.portfolio)

            pdt.assert_frame_equal(s.portfolio.weight, test_portfolio().weights)
            pdt.assert_frame_equal(s.portfolio.price, test_portfolio().prices)

            self.assertEqual(s.portfolio.last_valid, pd.Timestamp("2015-04-22"))
            self.assertListEqual(s.assets, ['A', 'B', 'C', 'D', 'E', 'F', 'G'])

            # upsert an existing portfolio

            p = test_portfolio().tail(10)
            # double the weights for the last 5 days of the existing portfolio
            x = s.upsert(portfolio=2*p, days=5)
            # check for the jump in leverage
            self.assertAlmostEqual(x.leverage.loc["2015-04-16"], 0.3121538556, places=7)
            self.assertAlmostEqual(x.leverage.loc["2015-04-17"], 0.6213015319, places=7)

    def test_portfolio(self):
        portfolio = test_portfolio()

        p = PortfolioSQL(name="test")
        p.upsert(portfolio=portfolio)
        pdt.assert_frame_equal(p.price, test_portfolio().prices)
        pdt.assert_frame_equal(p.weight, test_portfolio().weights)
        self.assertEqual(p.assets, test_portfolio().assets)
        self.assertEqual(p.last_valid, test_portfolio().index[-1])

        # test the truncation
        p1 = portfolio.truncate(after=pd.Timestamp("2015-01-01") - pd.DateOffset(seconds=1))
        pp = PortfolioSQL(name="wurst")

        pp.upsert(portfolio=test_portfolio().truncate(after=pd.Timestamp("2015-02-01")))
        self.assertEqual(p1.index[-1], pd.Timestamp("2014-12-31"))
        p2 = portfolio.truncate(before=pd.Timestamp("2015-01-01").date())

        pp.upsert(portfolio=p2)
        pdt.assert_frame_equal(pp.weight, test_portfolio().weights)

