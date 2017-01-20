from unittest import TestCase

import pandas as pd

from pyutil.mongo.csvArchive import CsvArchive
from test.config import read_frame
from test.test_strategy.scripts.script_a import Configuration


class TestCconfigMaster(TestCase):
    def test_master(self):
        archive = CsvArchive(PX_LAST=read_frame("price.csv", parse_dates=True))

        configuration = Configuration(archive, t0=pd.Timestamp("2013-01-01"))
        self.assertEquals(configuration.name, "test_a")
        self.assertEquals(configuration.group, "testgroup_a")

        portfolio = configuration.portfolio()
        self.assertAlmostEquals(portfolio.nav.sharpe_ratio(), -0.27817227635204395, places=5)
        self.assertEquals(configuration.empty(), False)
        self.assertEquals(configuration.count(), 3)

    def test_empty(self):
        archive = CsvArchive(PX_LAST=read_frame("price.csv", parse_dates=True))
        configuration = Configuration(archive, t0=pd.Timestamp("2013-01-01"))
        configuration.assets = []

        with self.assertRaises(AssertionError):
            configuration.prices()

