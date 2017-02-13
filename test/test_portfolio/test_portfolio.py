
import pandas as pd
import numpy as np
import pandas.util.testing as pdt

import matplotlib as mpl
mpl.use('Agg')

from pyutil.portfolio.portfolio import Portfolio, read_csv
from test.config import test_portfolio, read_frame, resource
from unittest import TestCase

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
        x = portfolio.top_flop(day_final=pd.Timestamp("2015-01-01"))
        self.assertAlmostEqual(x["Value"].values[16], 0.00025637273414469419, places=5)
        self.assertEqual(x.index.names[0], "category")
        self.assertEqual(x.index.names[1], "rank")

    def test_tail(self):
        x = portfolio.tail(5)
        self.assertEqual(len(x.index), 5)
        self.assertEqual(x.index[0], pd.Timestamp("2015-04-16"))

    def test_sector_weights(self):
        x = portfolio.sector_weights(pd.Series({"A": "A", "B": "A", "C": "B", "D": "B",
                                                "E": "C", "F": "C", "G": "C"}))

        pdt.assert_frame_equal(x.head(10), read_frame("sector_weights.csv", parse_dates=True))

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

    def test_plot(self):
        x = portfolio.plot()
        self.assertEqual(len(x.get_axes()), 3)

    def test_iron_threshold(self):
        p1 = test_portfolio().iron_threshold(threshold=0.05)
        self.assertEqual(len(p1.trading_days), 40)

    def test_iron_time(self):
        p2 = test_portfolio().iron_time(rule="3M")
        self.assertEqual(len(p2.trading_days), 10)


    def test_transaction_report(self):
        p1 = test_portfolio().iron_threshold(threshold=0.05)
        y = p1.transaction_report()
        yy = read_frame("report.csv", index_col=[0, 1])
        yy.index.names = [None, None]
        pdt.assert_frame_equal(y, yy)


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

    def test_mtd(self):
        portfolio = test_portfolio()
        p = portfolio.mtd(today=portfolio.index[-1])
        self.assertEqual(p.index[0], pd.Timestamp("2015-04-01"))

    def test_ytd(self):
        portfolio = test_portfolio()
        p = portfolio.ytd(today=portfolio.index[-1])
        self.assertEqual(p.index[0], pd.Timestamp("2015-01-01"))

    def test_state(self):
        p = test_portfolio()
        x = p.state
        self.assertAlmostEqual(x["Extrapolated"]["F"], 3.6564581863077144, places=10)
        self.assertAlmostEqual(x["Gap"]["A"], 0.042612879799229508, places=10)

    def test_empty(self):
        p = Portfolio(prices=pd.DataFrame(columns=["A"]), weights=pd.DataFrame(columns=["A"]))
        self.assertTrue(p.empty)

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
            Portfolio(prices=pd.DataFrame(index=[1, 1]), weights=pd.DataFrame(index=[1, 1]))

    def test_series(self):
        prices=pd.DataFrame(index=[0,1,2],columns=["A","B"])
        weights=pd.Series(index=["A","B"],data=[1.0,1.0])
        p=Portfolio(prices=prices, weights=weights)
        self.assertEquals(p.weights["B"][2],1)

    def test_gap(self):
        prices=pd.DataFrame(index=[0,1,2,3], columns=["A"], data=100)
        weights=pd.DataFrame(index=[0,1,2,3], columns=["A"], data=[1,np.nan,1,1])
        with self.assertRaises(AssertionError):
            Portfolio(prices=prices, weights=weights)

    def test_meta(self):
        p = test_portfolio()
        p1 = Portfolio(p.prices, p.weights, Peter="Maffay", Comment="Nur Du")
        self.assertDictEqual(p1.meta, {"Peter": "Maffay", "Comment": "Nur Du"})
        p1.meta["Peter"] = "Haha"
        self.assertDictEqual(p1.meta, {"Peter": "Haha", "Comment": "Nur Du"})

        p2 = Portfolio(p.prices, p.weights)
        p2.meta["Wurst"] = 1
        self.assertDictEqual(p2.meta, {"Wurst": 1})

    #def test_csv(self):
    #    test_portfolio().to_csv(resource("hans.csv"))
        #assert False

    def test_csv_back(self):
        p = test_portfolio()
        #p.to_csv("hans.csv")
        portfolio = read_csv(resource("hans.csv"))
        pdt.assert_frame_equal(portfolio.weights, p.weights)
        pdt.assert_frame_equal(portfolio.prices, p.prices)

