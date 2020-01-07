import pandas as pd
import pandas.util.testing as pdt
from mongoengine import *

from pyutil.mongo.engine.pandasdocument import PandasDocument
from pyutil.timeseries.merge import merge
from test.config import mongo_client


class Singer(PandasDocument):
    pass


class TestEngine(object):
    def test_document(self, mongo_client):
        # Create a new page and add tags
        p = Singer(name="Peter Maffay")
        assert p.name == "Peter Maffay"

    def test_lt(self):
        assert Singer(name="A") < Singer(name="B")

    def test_reference(self):
        p = Singer(name="Peter Maffay")
        assert p.reference.get("NoNoNo", default=5) == 5

        p.reference["XXX"] = 10
        assert p.reference.keys() == {"XXX"}
        assert {k: v for k, v in p.reference.items()} == {"XXX": 10}

    def test_equals(self):
        p1 = Singer(name="Peter Maffay")
        p2 = Singer(name="Peter Maffay")

        assert p1 == p2

    def test_merge(self):
        p = Singer(name="Peter Maffay")

        ts1 = pd.Series(index=[1,2], data=[3.3, 4.3])
        ts2 = pd.Series(index=[2,3], data=[5.3, 6.3])

        p.close = ts1
        p.close = merge(old=p.close, new=ts2)

        pdt.assert_series_equal(p.close, pd.Series(index=[1,2,3], data=[3.3, 5.3, 6.3]))

