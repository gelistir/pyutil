from unittest import TestCase

from pyutil.mongo.asset import Asset
from pyutil.mongo.assets import Assets

from test.config import read_frame
from test.test_strategy.scripts.script_a import Configuration


class TestConfigMaster(TestCase):
    def test_master(self):
        symbols = read_frame("symbols.csv")
        prices = read_frame("price.csv")
        assets = Assets([Asset(name, data=prices[name].to_frame(name="PX_LAST"), **symbols.ix[name].to_dict()) for name in prices.keys()])

        # only hand over a function and evaluate on demand rather than creating a monster object in memory
        configuration = Configuration(reader=assets.reader)

        self.assertEquals(configuration.name, "test_a")
        self.assertEquals(configuration.group, "testgroup_a")

        portfolio = configuration.portfolio()
        self.assertAlmostEquals(portfolio.nav.sharpe_ratio(), -0.27817227635204395, places=5)


