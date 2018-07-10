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

    def test_write_portfolio(self):
        p1 = test_portfolio()
        self.client.write_portfolio(portfolio=p1, name="test-a")
        self.client.write_portfolio(portfolio=p1, name="test-b")
        self.assertSetEqual(self.client.portfolios, {"test-a", "test-b"})

        #self.assertListEqual(self.client.tag_keys(measurement="prices"), ["name"])

        assert "prices" in self.client.measurements
        assert "weights" in self.client.measurements

        #for a in self.client.query("""SHOW FIELD KEYS FROM prices""").get_points():
        #    print("hello")
        #    print(a)

        x = self.client.query("SELECT * FROM prices")["prices"].set_index("name", append=True).swaplevel().loc["test-a"].tz_localize(None)
        pdt.assert_frame_equal(x, test_portfolio().prices)

    def test_write_series(self):
        nav = test_portfolio().nav
        nav.name = "nav"
        self.client.write_series(ts=nav, tags={"name": "test-a"}, field="nav", measurement="nav")
        pdt.assert_series_equal(nav, self.client.read_series(field="nav", measurement="nav", conditions={"name": "test-a"}))

        # alternative way to read the series
        x = self.client.read_series(field="nav", measurement="nav", tags=["name"])
        pdt.assert_series_equal(nav, x.unstack()["test-a"], check_names=False)


    def test_read_write_portfolio(self):
        p1 = test_portfolio()
        self.client.write_portfolio(portfolio=p1, name="test-a")

        p, w = self.client.read_portfolio(name="test-a")

        pdt.assert_frame_equal(p, test_portfolio().prices)
        pdt.assert_frame_equal(w, test_portfolio().weights)
