from unittest import TestCase

import pandas as pd

from pyutil.mongo.portfolios import Portfolios
from test.config import read_frame, test_portfolio

symbols = read_frame("symbols.csv")


class TestPortfolios(TestCase):
    @classmethod
    def setUp(self):
        self.portfolios = Portfolios()
        p = test_portfolio(group="test", comment="test", time=pd.Timestamp("1980-01-01"))
        self.portfolios["test"] = p


    def test_assets_add(self):
        r = self.portfolios.nav()["test"]
        # test the nav
        self.assertAlmostEqual(r["2015-04-22"], 1.0070191775792583, places=5)

        r = self.portfolios.strategies
        self.assertEqual(r["group"]["test"], "test")

    def test_sector_weights(self):
        symbolmap = symbols["group"]
        sector_w = self.portfolios.sector_weights(symbolmap)
        self.assertAlmostEqual(sector_w["Equity"]["test"], 21.437411662111511, places=5)

    def test_ytd(self):
        m = self.portfolios.ytd(today=pd.Timestamp("2015-04-22").date())
        self.assertAlmostEqual(m["Apr"]["test"], 1.4133604922211385, places=10)

    def test_mtd(self):
        m = self.portfolios.mtd(today=pd.Timestamp("2015-04-22").date())
        self.assertAlmostEqual(m["Apr 10"]["test"], 0.26611289332396648, places=10)

    def test_recent(self):
        m = self.portfolios.recent
        self.assertAlmostEqual(m["Apr 10"]["test"], 0.26611289332396648, places=10)

    def test_period_returns(self):
        m = self.portfolios.period_returns
        self.assertAlmostEqual(m["Month-to-Date"]["test"], 1.4133604922211385, places=10)

        #asset = Asset(name="Peter Maffay", data=prices, b=3.0, a=2.0)
        #portfolios.add(asset)

        #self.assertSetEqual(set(portfolios.keys()), {"Peter Maffay"})
        #pdt.assert_frame_equal(portfolios["Peter Maffay"].data(), prices)
        #pdt.assert_series_equal(portfolios["Peter Maffay"].reference, pd.Series(index=["a", "b"], data=[2.0, 3.0]))

        #self.assertEquals(len(portfolios), 1)

        #pdt.assert_frame_equal(portfolios.reference, pd.DataFrame(index=["Peter Maffay"], columns=["a", "b"], data=[[2.0, 3.0]]))

        #self.assertEquals(str(portfolios), "Asset Peter Maffay with series ['A', 'B', 'C', 'D', 'E', 'F', 'G'] and reference [('a', 2.0), ('b', 3.0)]")

