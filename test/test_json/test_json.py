from unittest import TestCase

from pyutil.json.json import series2dict, frame2dict, flatten
from test.config import read_frame


class TestJson(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.assets = read_frame("price.csv")

    def test_series2dict(self):
        ts = self.assets["A"]
        x = series2dict(ts)
        self.assertAlmostEqual(x["20130625"], 1277.650, places=5)

    def test_frame2dict(self):
        x = frame2dict(self.assets)
        self.assertAlmostEqual(x["A"]["20130625"], 1277.650, places=5)

    def test_flatten(self):
        x = flatten(name="test", ts=self.assets["A"])
        self.assertAlmostEqual(x["test.20130625"], 1277.650, places=5)

    def test_flatten_frame(self):
        x = flatten(name="test", ts=self.assets.stack())
        self.assertAlmostEqual(x["test.A.20130625"], 1277.650, places=5)
