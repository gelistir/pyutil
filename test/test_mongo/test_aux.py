import pandas as pd

from unittest import TestCase
from pyutil.mongo.aux import series2dict, flatten


class TestAux(TestCase):
    def test_series2dict(self):
        x = pd.Series(index=[pd.Timestamp("2014-01-01"), pd.Timestamp("2014-02-26")], data=[1.0, 3.0])
        self.assertDictEqual(series2dict(x), {'20140101': 1.0, '20140226': 3.0})

    def test_flatten_series(self):
        x = pd.Series(index=[pd.Timestamp("2014-01-01"), pd.Timestamp("2014-02-26")], data=[1.0, 3.0])
        self.assertDictEqual(flatten("test", x), {'$set': {'test.20140226': 3.0, 'test.20140101': 1.0}})

    def test_flatten_frame(self):
        x = pd.Series(index=[pd.Timestamp("2014-01-01"), pd.Timestamp("2014-02-26")], data=[1.0, 3.0])
        frame = pd.DataFrame({"peter": x})
        self.assertDictEqual(flatten("maffay", frame), {'$set': {'maffay.peter.20140226': 3.0, 'maffay.peter.20140101': 1.0}})




