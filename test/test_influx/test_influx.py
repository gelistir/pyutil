from unittest import TestCase

import pandas.util.testing as pdt

from pyutil.influx.client import Client
from test.config import test_portfolio


class TestInfluxDB(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client(host='test-influxdb', database="testexample3")

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

        p,w = self.client.read_portfolio(name="test-a")

        pdt.assert_frame_equal(p, test_portfolio().prices)
        pdt.assert_frame_equal(w, test_portfolio().weights)

        self.assertListEqual(self.client.tag_keys(measurement="prices"), ["name"])

        assert "prices" in self.client.measurements
        assert "weights" in self.client.measurements

        for a in self.client.query("""SHOW FIELD KEYS FROM prices""").get_points():
            print("hello")
            print(a)

        for a in self.client.query("""SHOW TAG VALUES FROM prices WITH KEY = "name" """).get_points():
            print(a)

        print(self.client.tag_values(measurement="prices", key="name"))

        print(self.client.query("""SELECT *::field FROM prices""")["prices"].tz_localize(None))

        print(self.client.read_frame(measurement="prices", tags=["name"], conditions=[("name","test-a")]))
        print(self.client.read_frame(measurement="prices", conditions=[("name","test-a")]))
        print(self.client.read_frame(measurement="prices"))

    def test_write_series(self):
        nav = test_portfolio().nav

        #self.client.write_frame(nav.to_frame(name="nav"), measurement="nav", tags={"name": "test-a"})
        self.client.write_series(ts=nav, tags={"name": "test-a"}, field="nav", measurement="nav")

        print(self.client.read_series(measurement="nav", field="nav"))
