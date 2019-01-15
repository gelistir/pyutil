from unittest import TestCase

import numpy as np
import pandas as pd
import pandas.util.testing as pdt

from pyutil.portfolio.portfolio import Portfolio, similar
from pyutil.test.aux import read_frame
from test.config import test_portfolio, resource

portfolio = test_portfolio()


class TestPortfolio(TestCase):
    def test_leverage(self):
        self.assertAlmostEqual(portfolio.leverage["2013-07-19"], 0.25505730106555635, places=5)
        self.assertAlmostEqual(portfolio.nav["2013-07-19"], 0.9849066065468487, places=5)
        self.assertAlmostEqual(portfolio.cash["2015-04-22"], 0.69102612448658074, places=5)

        # test the set...
        self.assertSetEqual(set(portfolio.assets), {'A', 'B', 'C', 'D', 'E', 'F', 'G'})

    def test_truncate(self):
        self.assertEqual(portfolio.truncate(before="2013-01-08").index[0], pd.Timestamp("2013-01-08"))

    def test_top_flop(self):
        x = portfolio.top_flop_ytd(day_final=pd.Timestamp("2014-12-31"))
        self.assertAlmostEqual(x["top"].values[0], 0.024480763822820828, places=10)

        y = portfolio.top_flop_mtd(day_final=pd.Timestamp("2014-12-31"))
        self.assertAlmostEqual(y["flop"].values[0], -0.0040598440397091595, places=10)

    def test_tail(self):
        x = portfolio.tail(5)
        self.assertEqual(len(x.index), 5)
        self.assertEqual(x.index[0], pd.Timestamp("2015-04-16"))

    def test_sector_weights(self):
        x = portfolio.sector_weights(symbolmap=pd.Series({"A": "A", "B": "A", "C": "B", "D": "B",
                                                "E": "C", "F": "C", "G": "C"}), total=True)

        pdt.assert_frame_equal(x.tail(10), read_frame(resource("sector_weights.csv"), parse_dates=True))

        x = portfolio.sector_weights_final(symbolmap=pd.Series({"A": "A", "B": "A", "C": "B", "D": "B",
                                                "E": "C", "F": "C", "G": "C"}), total=True)

        pdt.assert_series_equal(x, read_frame(resource("sector_weights.csv"), parse_dates=True).iloc[-1])


    def test_position(self):
        x = 1e6 * portfolio.position
        self.assertAlmostEqual(x["A"][pd.Timestamp("2015-04-22")], 60.191699988670969, places=5)

    def test_build_portfolio(self):
        prices = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3],
                              data=[[1000.0, 1000.0], [1500.0, 1500.0], [2000.0, 2000.0]])
        weights = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3], data=[[0.25, 0.25], [0.25, 0.25], [0.25, 0.25]])

        p = Portfolio(prices=prices, weights=weights)
        pdt.assert_frame_equal(prices, p.prices)

        self.assertAlmostEqual(p.position["A"][2], 0.00020833333333333335, places=5)

    def test_mul(self):
        print(2 * portfolio.weights)
        print((2 * portfolio).weights)
        pdt.assert_frame_equal(2 * portfolio.weights, (2 * portfolio).weights, check_names=False)

    def test_iron_threshold(self):
        p1 = test_portfolio().truncate(before="2015-01-01").iron_threshold(threshold=0.05)
        self.assertEqual(len(p1.trading_days), 5)

    def test_iron_time(self):
        p2 = test_portfolio().truncate(before="2014-07-01").iron_time(rule="3M")
        self.assertEqual(len(p2.trading_days), 4)

    def test_init_1(self):
        prices = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3], data=[[10.0, 10.0], [15.0, 15.0], [20.0, np.nan]])
        weights = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3], data=[[0.3, 0.7], [0.3, 0.7], [0.3, 0.7]])
        portfolio = Portfolio(prices=prices, weights=weights)
        self.assertAlmostEqual(0.3, portfolio.weights["A"][3], places=5)
        self.assertAlmostEqual(15.0, portfolio.prices["B"][3], places=5)

    def test_init_2(self):
        with self.assertRaises(AssertionError):
            prices = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3], data=[[10.0, 10.0], [15.0, 15.0], [20.0, np.nan]])
            weights = pd.DataFrame(columns=["A", "B"], index=[1.5], data=[[0.3, 0.7]])
            Portfolio(prices=prices, weights=weights)

    def test_init_3(self):
        with self.assertRaises(AssertionError):
            prices = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3], data=[[10.0, 10.0], [15.0, 15.0], [20.0, np.nan]])
            weights = pd.DataFrame(columns=["C"], index=[1.5], data=[[0.3]])
            Portfolio(prices=prices, weights=weights)

    def test_state(self):
        pdt.assert_frame_equal(test_portfolio().state, read_frame(resource("state2.csv")))

    def test_mismatch_columns(self):
        with self.assertRaises(AssertionError):
            Portfolio(prices=pd.DataFrame(columns=["A"]), weights=pd.DataFrame(columns=["B"]))

    def test_mismatch_index(self):
        with self.assertRaises(AssertionError):
            Portfolio(prices=pd.DataFrame(index=[0]), weights=pd.DataFrame(index=[1]))

    def test_monotonic_index(self):
        with self.assertRaises(AssertionError):
            Portfolio(prices=pd.DataFrame(index=[1,0]), weights=pd.DataFrame(index=[1,0]))

    def test_duplicates_index(self):
        with self.assertRaises(AssertionError):
            Portfolio(prices=pd.DataFrame(index=[1, 1]))

    def test_series(self):
        prices=pd.DataFrame(index=[0,1,2],columns=["A","B"])
        weights=pd.Series(index=["A","B"],data=[1.0,1.0])
        p=Portfolio(prices=prices, weights=weights)
        self.assertEqual(p.weights["B"][2],1)

    def test_gap(self):
        prices=pd.DataFrame(index=[0,1,2,3], columns=["A"], data=100)
        weights=pd.DataFrame(index=[0,1,2,3], columns=["A"], data=[1,np.nan,1,1])
        with self.assertRaises(AssertionError):
            Portfolio(prices=prices, weights=weights)

    def test_weight_current(self):
        p = test_portfolio()
        self.assertAlmostEqual(p.weight_current["D"], 0.022837914929098344, places=10)

    def test_subportfolio(self):
        p = test_portfolio()
        sub = p.subportfolio(assets=p.assets[:2])
        self.assertListEqual(p.assets[:2], sub.assets)

    def test_snapshot(self):
        p = test_portfolio()
        x = p.snapshot(n=5)
        self.assertAlmostEqual(x["Year-to-Date"]["B"], 0.01615087992272124, places=10)

    def test_apply(self):
        p = test_portfolio()
        w = p.apply(lambda x: 2*x)
        pdt.assert_frame_equal(w.weights, 2*p.weights)

    def test_similar(self):
        p = test_portfolio()
        self.assertFalse(similar(p, 5))
        self.assertFalse(similar(p, p.subportfolio(assets=["A","B","C"])))
        self.assertFalse(similar(p, p.tail(100)))
        x = test_portfolio()
        p2 = Portfolio(weights=2*x.weights, prices=x.prices)
        self.assertFalse(similar(p, p2))

        self.assertTrue(similar(p, p))
