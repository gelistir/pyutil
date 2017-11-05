from unittest import TestCase

from pyutil.mongo.asset import Asset

from test.config import read_frame
from test.test_strategy.scripts.script_a import Configuration


class TestConfigMaster(TestCase):
    def test_master(self):
        prices = read_frame("price.csv")

        # only hand over a function to read individual assets.
        # This avoids reading the entire database! The strategy will extract only what it needs
        def f_asset(name):
            return Asset(name, prices[name].to_frame(name="PX_LAST"))

        configuration = Configuration(reader=f_asset)

        portfolio = configuration.portfolio()
        self.assertAlmostEqual(portfolio.nav.sharpe_ratio(), -0.27817227635204395, places=5)

        x = configuration.reader("A")
        self.assertTrue(isinstance(x, Asset))


