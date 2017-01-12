from unittest import TestCase

import pandas as pd

from pyutil.mongo.mongoArchive import _mongo, _flatten
from test.config import read_frame


class TestJson(TestCase):
    def test_series2dict(self):
        x = _mongo(x=read_frame("price.csv")["A"])
        self.assertAlmostEqual(x["20130625"], 1277.650, places=5)

    def test_frame2dict(self):
        x = _mongo(x=read_frame("price.csv"))
        self.assertAlmostEqual(x["A"]["20130625"], 1277.650, places=5)

    def test_flatten(self):
        x = _flatten({"test": _mongo(x=read_frame("price.csv")["A"])})
        self.assertAlmostEqual(x["test.20130625"], 1277.650, places=5)

    def test_flatten_frame(self):
        x = _flatten({"test": _mongo(x=read_frame("price.csv"))})
        self.assertAlmostEqual(x["test.A.20130625"], 1277.650, places=5)

    def test_warning(self):
        with self.assertWarns(Warning):
            _mongo(x=pd.Series(index=["20160101"],data=[2.0]))
