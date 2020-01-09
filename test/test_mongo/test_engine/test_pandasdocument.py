import pandas as pd
import pandas.util.testing as pdt
import pytest

from pyutil.mongo.engine.pandasdocument import PandasDocument
from pyutil.timeseries.merge import merge
from test.config import mongo_client


class Singer(PandasDocument):
    pass


class TestEngine(object):
    def test_document(self):
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

        ts1 = pd.Series(index=[1, 2], data=[3.3, 4.3])
        ts2 = pd.Series(index=[2, 3], data=[5.3, 6.3])

        p.close = ts1
        p.close = merge(old=p.close, new=ts2)

        pdt.assert_series_equal(p.close, pd.Series(index=[1, 2, 3], data=[3.3, 5.3, 6.3]))

    def test_products(self, mongo_client):
        p1 = Singer(name="Peter")
        p1.save()

        p2 = Singer(name="Falco")
        p2.save()

        # here we query the database! Hence need the client in the background
        a = Singer.products(names=["Peter"])
        assert len(a) == 1
        assert a[0] == p1

        b = Singer.products()
        assert len(b) == 2
        assert set(b) == {p1, p2}

        frame = Singer.reference_frame(products=[p1, p2])
        assert set(frame.index) == {"Peter", "Falco"}
        assert frame.empty

    def test_reference_frame(self):
        p1 = Singer(name="Peter")
        p1.reference["A"] = 20.0

        p2 = Singer(name="Falco")
        p2.reference["A"] = 30.0
        p2.reference["B"] = 10.0

        frame = Singer.reference_frame(products=[p1, p2])
        assert set(frame.index) == {"Peter", "Falco"}
        assert set(frame.keys()) == {"A", "B"}

    def test_ts(self):
        p1 = Singer(name="Peter Maffay")
        p1.price = pd.Series(data=[2, 3, 5])

        p2 = Singer(name="Falco")

        assert p1.reference == {}
        frame = Singer.pandas_frame(item="price", products=[p1, p2])

        assert set(frame.keys()) == {"Peter Maffay"}
        pdt.assert_series_equal(p1.price, frame["Peter Maffay"], check_names=False)

    def test_pandas_wrong(self):
        p1 = Singer(name="Peter Maffay")
        p1.price = 5.0
        assert Singer.pandas_frame(item="price", products=[p1]).empty

        #with pytest.raises(AttributeError):
        #    Singer.pandas_frame(key="price", products=[p1])

    def test_last(self):
        p1 = Singer(name="Peter Maffay")
        assert p1.last(item="price") is None
        assert p1.last(item="price", default=pd.Timestamp("2010-01-01")) == pd.Timestamp("2010-01-01")

    def test_pandas(self):
        p1 = Singer(name="Peter Maffay")
        assert p1.pandas(item="price") is None
        x = p1.pandas(item="price", default=pd.Series({}))
        assert x.empty

        #assert pdt.assert_series_equal(p1.pandas(item="price", default=pd.Series({})), pd.Series({}))