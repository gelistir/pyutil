from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.influx.client import test_client
from pyutil.performance.summary import fromNav
from test.config import test_portfolio


class TestInfluxDB(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = test_client()
        cls.client.recreate(dbname="test")

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database("test")
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
        self.client.write(frame=nav.to_frame(name="nav"), tags={"name": "test-a", "singer": "Peter Maffay"}, measurement="nav")
        pdt.assert_series_equal(nav, self.client.read(field="nav", measurement="nav", conditions={"name": "test-a"})["nav"].dropna(), check_names=False)

        # alternative way to read the series
        x = self.client.read(field="nav", measurement="nav", tags=["name"])["nav"].dropna()
        y = x.xs(key="test-a", level="name", drop_level=True)

        pdt.assert_series_equal(nav, y.dropna(), check_names=False)
        assert "nav" in self.client.measurements


    def test_write_series_date(self):
        x = pd.Series({pd.Timestamp("1978-11-12").date(): 5.1})
        self.client.write(frame=x.to_frame(name="temperature"), tags={"name": "birthday"}, measurement="navxx")
        y = self.client.read(field="temperature", measurement="navxx", conditions={"name": "birthday"})["temperature"].dropna()
        pdt.assert_series_equal(y, pd.Series({pd.Timestamp("1978-11-12"): 5.1}, name="temperature"), check_names=False)

        z = self.client.read(field="*", tags=None, measurement="navxx")
        pdt.assert_frame_equal(z, pd.DataFrame(index=[pd.Timestamp("1978-11-12")], data=[["birthday",5.1]],columns=["name","temperature"]), check_names=False)
        #assert False

    def test_write_frame(self):
        nav = test_portfolio().nav
        self.client.write(frame=nav.to_frame(name="maffay"), tags={"name": "test-a"}, measurement="nav2")
        self.client.write(frame=nav.tail(20).to_frame(name="maffay"), tags={"name": "test-b"}, measurement="nav2")

        frame = self.client.read(field="maffay", measurement="nav2", tags=["name"])["maffay"].dropna().unstack()

        pdt.assert_series_equal(fromNav(frame["test-a"]), nav, check_names=False)
        pdt.assert_series_equal(fromNav(frame["test-b"]).dropna(), nav.tail(20), check_names=False)


    def test_stack(self):
        c = test_client()
        with c as client:
            print("hello")

    def test_repeat(self):
        nav = test_portfolio().nav.dropna()
        nav.name = "nav"

        self.client.write(frame=nav.to_frame(name="wurst"), tags={"name": "test-wurst"}, measurement="navx")
        y = self.client.read(field="wurst", tags=["name"], measurement="navx")["wurst"].dropna()

        # write the entire series again!
        self.client.write(frame=nav.to_frame(name="wurst"), tags={"name": "test-wurst"}, measurement="navx")
        x = self.client.read(field="wurst", tags=["name"], measurement="navx")["wurst"].dropna()

        self.assertEqual(len(x), len(y))
