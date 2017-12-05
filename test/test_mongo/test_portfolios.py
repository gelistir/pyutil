from unittest import TestCase

from pyutil.mongo.portfolios import Portfolios
from test.config import read_frame, test_portfolio

symbols = read_frame("symbols.csv")


class TestPortfolios(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.portfolios = Portfolios({"test": test_portfolio()})

    def test_assets_add(self):
        r = self.portfolios.nav()["test"]
        # test the nav
        self.assertAlmostEqual(r["2015-04-22"], 1.0070191775792583, places=5)

    def test_empty(self):
        p = Portfolios({})
        self.assertTrue(p.empty)

    def test_str(self):
        self.assertEqual(str(self.portfolios), "Portfolio with assets: ['A', 'B', 'C', 'D', 'E', 'F', 'G']")



