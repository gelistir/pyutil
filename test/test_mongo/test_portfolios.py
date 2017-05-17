from unittest import TestCase

from pyutil.mongo.portfolios import Portfolios
from test.config import read_frame, test_portfolio

symbols = read_frame("symbols.csv")


class TestPortfolios(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.portfolios = Portfolios()
        p = test_portfolio()
        cls.portfolios["test"] = p

    def test_assets_add(self):
        r = self.portfolios.nav()["test"]
        # test the nav
        self.assertAlmostEqual(r["2015-04-22"], 1.0070191775792583, places=5)

    def test_sector_weights(self):
        symbolmap = symbols["group"]
        sector_w = self.portfolios.sector_weights(symbolmap)
        self.assertAlmostEqual(sector_w["Equity"]["test"], 21.437411662111511, places=5)

    def test_ytd(self):
        m = self.portfolios.ytd
        self.assertAlmostEqual(m["Apr"]["test"], 1.4133604922211385, places=10)

    def test_mtd(self):
        m = self.portfolios.mtd
        self.assertAlmostEqual(m["Apr 10"]["test"], 0.26611289332396648, places=10)

    def test_recent(self):
        m = self.portfolios.recent()
        self.assertAlmostEqual(m["Apr 10"]["test"], 0.26611289332396648, places=10)

    def test_period_returns(self):
        m = self.portfolios.period_returns
        self.assertAlmostEqual(m["Month-to-Date"]["test"], 1.4133604922211385, places=10)
