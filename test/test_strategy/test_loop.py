from unittest import TestCase

#from pyutil.mongo.assets import frame2assets
from pyutil.mongo.asset import Asset
from pyutil.mongo.assets import Assets
from pyutil.strategy.Loop import loop_configurations
from test.config import read_frame

#assets = frame2assets(symbols=read_frame("symbols.csv"), frame=read_frame("price.csv", parse_dates=True))


class TestLoopConfigurations(TestCase):
    def test_loop_config(self):
        symbols = read_frame("symbols.csv")
        prices = read_frame("price.csv")
        assets = Assets([Asset(name, data=prices[name].to_frame(name="PX_LAST"), **symbols.ix[name].to_dict()) for name in prices.keys()])

        path = "/pyutil/test/test_strategy/scripts"
        results = {r.name: r for r in loop_configurations(reader=assets.reader, path=path, prefix="test.test_strategy.scripts.")}
        self.assertSetEqual(set(results.keys()), {"test_a", "test_b"})
        self.assertAlmostEqual(results["test_a"].portfolio.nav.sharpe_ratio(), -0.27817227635204395, places=5)
        self.assertAlmostEqual(results["test_b"].portfolio.nav.sharpe_ratio(), -0.58967955070885469, places=5)

