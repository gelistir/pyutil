from unittest import TestCase
from pyutil.mongo.mongoArchive import _mongo_series, _mongo_frame, _flatten
from test.config import read_frame


class TestJson(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.assets = read_frame("price.csv")

    def test_series2dict(self):
        x = _mongo_series(x=self.assets["A"])
        self.assertAlmostEqual(x["20130625"], 1277.650, places=5)

    def test_frame2dict(self):
        x = _mongo_frame(x=self.assets)
        self.assertAlmostEqual(x["A"]["20130625"], 1277.650, places=5)

    def test_flatten(self):
        x = _flatten({"test": _mongo_series(x=self.assets["A"])})
        self.assertAlmostEqual(x["test.20130625"], 1277.650, places=5)

    def test_flatten_frame(self):
        x = _flatten({"test": _mongo_frame(x=self.assets)})
        self.assertAlmostEqual(x["test.A.20130625"], 1277.650, places=5)
