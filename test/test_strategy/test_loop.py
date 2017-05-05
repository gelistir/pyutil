from unittest import TestCase

from pyutil.mongo.asset import Asset
from pyutil.strategy.Loop import loop_configurations
from test.config import read_frame


class TestLoopConfigurations(TestCase):
    def test_loop_config(self):
        prices = read_frame("price.csv")

        # we need the reader function...
        reader = lambda name: Asset(name, data=prices[name].to_frame(name="PX_LAST"))

        path = "/pyutil/test/test_strategy/scripts"
        results = {name: portfolio for name, portfolio in loop_configurations(reader=reader, path=path, prefix="test.test_strategy.scripts.")}
        self.assertSetEqual(set(results.keys()), {"test_a", "test_b"})
        self.assertAlmostEqual(results["test_a"].nav.sharpe_ratio(), -0.27817227635204395, places=5)
        self.assertAlmostEqual(results["test_b"].nav.sharpe_ratio(), -0.58967955070885469, places=5)

