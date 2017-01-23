from unittest import TestCase

from pyutil.mongo.csvArchive import CsvArchive
from pyutil.strategy.Loop import loop_configurations
from test.config import read_frame



class TestLoopConfigurations(TestCase):
    def test_loop_config(self):
        path = "/pyutil/test/test_strategy/scripts"
        archive = CsvArchive(PX_LAST=read_frame("price.csv", parse_dates=True))
        results = {r.name: r for r in loop_configurations(archive=archive, path=path, prefix="test.test_strategy.scripts.")}
        self.assertSetEqual(set(results.keys()), {"test_a", "test_b"})
        self.assertAlmostEqual(results["test_a"].portfolio.nav.sharpe_ratio(), -0.27817227635204395, places=5)
        self.assertAlmostEqual(results["test_b"].portfolio.nav.sharpe_ratio(), -0.58967955070885469, places=5)

