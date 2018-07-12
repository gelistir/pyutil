import pandas as pd
from unittest import TestCase

import pandas.util.testing as pdt

from pyutil.influx.client import Client
from test.config import test_portfolio


class TestInfluxDB(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client(host='test-influxdb', database="testexample2")
        print(dir(cls.client))


    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def test_client(self):
        databases = self.client.databases
        self.assertTrue("testexample2" in databases)

    def test_write_series(self):
        nav = test_portfolio().nav
        nav.name = "nav"
        self.client.write_series(ts=nav, tags={"name": "test-a"}, field="nav", measurement="nav")
        pdt.assert_series_equal(nav, self.client.read_series(field="nav", measurement="nav", conditions={"name": "test-a"}))

        # alternative way to read the series
        x = self.client.read_series(field="nav", measurement="nav", tags=["name"])
        pdt.assert_series_equal(nav, x.unstack()["test-a"].dropna(), check_names=False)

    def test_write_series_date(self):
        x = pd.Series({pd.Timestamp("1978-11-12").date(): 5.1})
        self.client.write_series(ts = x, tags={"name": "birthday"}, field="temperature", measurement="nav")
        y = self.client.read_series(field="temperature", measurement="nav", conditions={"name": "birthday"})
        pdt.assert_series_equal(y, pd.Series({pd.Timestamp("1978-11-12"): 5.1}, name="temperature"))
