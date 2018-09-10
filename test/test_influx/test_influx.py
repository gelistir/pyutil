import pandas as pd
from unittest import TestCase

import pandas.util.testing as pdt

from pyutil.influx.client import Client
from pyutil.performance.summary import fromNav
from test.config import test_portfolio


class TestInfluxDB(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client()
        cls.client.recreate(dbname="test")

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def test_host(self):
        self.assertEqual(self.client.host, "test-influxdb")

    def test_database(self):
        self.assertEqual("test", self.client.database)

    def test_port(self):
        self.assertEqual(self.client.port, 8086)

    def test_repr(self):
        self.assertEqual(str(self.client), "InfluxClient at test-influxdb on port 8086")

    def test_client(self):
        databases = self.client.databases
        self.assertTrue("test" in databases)

    def test_write_series(self):
        nav = test_portfolio().nav
        nav.name = "nav"
        self.client.write_series(ts=nav, tags={"name": "test-a"}, field="nav", measurement="nav")
        pdt.assert_series_equal(nav, self.client.read_series(field="nav", measurement="nav", conditions={"name": "test-a"}), check_names=False)

        # alternative way to read the series
        x = self.client.read_series(field="nav", measurement="nav", tags=["name"])
        y = x.xs(key="test-a", level="name", drop_level=True)

        pdt.assert_series_equal(nav, y.dropna(), check_names=False)
        assert "nav" in self.client.measurements


    def test_write_series_date(self):
        x = pd.Series({pd.Timestamp("1978-11-12").date(): 5.1})
        self.client.write_series(ts = x, tags={"name": "birthday"}, field="temperature", measurement="nav")
        y = self.client.read_series(field="temperature", measurement="nav", conditions={"name": "birthday"})
        pdt.assert_series_equal(y, pd.Series({pd.Timestamp("1978-11-12"): 5.1}, name="temperature"), check_names=False)

    def test_write_frame(self):
        nav = test_portfolio().nav
        self.client.write_series(ts=nav, tags={"name": "test-a"}, field="navframe", measurement="nav2")
        self.client.write_series(ts=nav.tail(20), tags={"name": "test-b"}, field="navframe", measurement="nav2")

        frame = self.client.read_series(field="navframe", measurement="nav2", tags=["name"]).unstack()

        pdt.assert_series_equal(fromNav(frame["test-a"]), nav, check_names=False)
        pdt.assert_series_equal(fromNav(frame["test-b"]).dropna(), nav.tail(20), check_names=False)




    #def test_read_frame(self):
    #    with self.assertRaises(KeyError):
    #        frame = self.client.read_series(field="DoesNotExist", measurement="nav2", tags=["name"])


    def test_stack(self):
        c = Client(database="wurst")
        with c as client:
            print("hello")

