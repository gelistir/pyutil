from unittest import TestCase

import pandas.util.testing as pdt

from pyutil.influx.client import Client
from test.config import test_portfolio


class TestInfluxDB(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client(host='test-influxdb', database="testexample")

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def test_client(self):
        databases = self.client.databases
        self.assertTrue("testexample" in databases)

    def test_write_series(self):
        nav = test_portfolio().nav
        nav.name = "nav"
        self.client.write_series(ts=nav, tags={"name": "test-a"}, field="nav", measurement="nav")
        pdt.assert_series_equal(nav, self.client.read_series(field="nav", measurement="nav", conditions={"name": "test-a"}))

        # alternative way to read the series
        x = self.client.read_series(field="nav", measurement="nav", tags=["name"])
        pdt.assert_series_equal(nav, x.unstack()["test-a"], check_names=False)
