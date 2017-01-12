import pandas as pd
from unittest import TestCase

from pyutil.mongo.csvArchive import CsvArchive
from pyutil.portfolio.portfolio import Portfolio
from pyutil.strategy.ConfigMaster import ConfigMaster
from test.config import read_frame


class Configuration(ConfigMaster):
    def __init__(self, archive, t0, logger=None):
        super().__init__(archive=archive, t0=t0, logger=logger)
        self.assets = ["A", "B", "C"]

    def portfolio(self):
        p = self.prices()
        return Portfolio(p, weights=pd.DataFrame(index=p.index, columns=p.keys(), data=1.0 / len(p.keys())))

    @property
    def name(self):
        return "test"

    @property
    def group(self):
        return "testgroup"



class TestCconfigMaster(TestCase):
    def test_master(self):
        archive = CsvArchive({"PX_LAST": read_frame("price.csv", parse_dates=True)})

        configuration = Configuration(archive, t0=pd.Timestamp("2013-01-01"))
        self.assertEqual(configuration.name, "test")
        self.assertEquals(configuration.group, "testgroup")

        portfolio=configuration.portfolio()
        self.assertAlmostEquals(portfolio.nav.sharpe_ratio(), -0.27817227635204395, places=5)
