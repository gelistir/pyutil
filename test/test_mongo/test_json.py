from unittest import TestCase
from pyutil.mongo.mongo_pandas import MongoSeries, MongoFrame
from test.config import read_frame


class TestJson(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.assets = read_frame("price.csv")

    def test_series2dict(self):
        x = MongoSeries(x=self.assets["A"]).mongo_dict()
        self.assertAlmostEqual(x["20130625"], 1277.650, places=5)

    def test_frame2dict(self):
        x = MongoFrame(x=self.assets).mongo_dict()
        self.assertAlmostEqual(x["A"]["20130625"], 1277.650, places=5)

    def test_flatten(self):
        x = MongoSeries(x=self.assets["A"]).mongo_dict(name="test")
        self.assertAlmostEqual(x["test.20130625"], 1277.650, places=5)

    def test_flatten_frame(self):
        x = MongoFrame(x=self.assets).mongo_dict(name="test")
        self.assertAlmostEqual(x["test.A.20130625"], 1277.650, places=5)
