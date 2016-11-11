import pandas as pd
from unittest import TestCase

from pyutil.mongo.archive import writer
from pyutil.portfolio.portfolio import Portfolio
from pyutil.strategy.ConfigMaster import ConfigMaster
from test.config import read_frame

import pandas.util.testing as pdt

class Configuration(ConfigMaster):
    def __init__(self, archive, t0, logger=None):
        super().__init__(archive=archive, t0=t0, logger=logger)
        self.configuration["prices"] = self.prices(["A", "B", "C"])

    def portfolio(self):
        p = self.configuration["prices"]
        return Portfolio(p, weights=pd.DataFrame(index=p.index, columns=p.keys(), data=1.0 / len(p.keys())))

    @property
    def name(self):
        return "test"

    @property
    def group(self):
        return "testgroup"



class TestCconfigMaster(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.writer = writer("tmp_ZHJKJFA8", host="mongo")
        cls.writer.update_assets(read_frame("price.csv", parse_dates=True))

    def test_master(self):

        con = Configuration(self.writer, t0=pd.Timestamp("2013-01-01"))
        self.assertEqual(con.name, "test")
        self.assertEquals(con.group, "testgroup")
        p = con.prices(assets=["A","B"])
        pdt.assert_frame_equal(p[["A","B"]], read_frame("price.csv")[["A","B"]].truncate(before=pd.Timestamp("2013-01-01")), check_less_precise=True)

        portfolio=con.portfolio()
        self.assertAlmostEquals(portfolio.nav.statistics.sharpe, -0.017204147680543617, places=5)



