import pandas as pd
import pytest

import pandas.util.testing as pdt

from pyutil.sql.base import Base
from pyutil.sql.product import Product


class Singer(Product, Base):
    def __init__(self, name):
        super().__init__(name)

@pytest.fixture()
def mongo():
    from mongomock import MongoClient
    return MongoClient().test

@pytest.fixture()
def ts():
    return pd.Series(data=[100, 200], index=[0, 1])

@pytest.fixture()
def singer(ts, mongo):
    s = Singer(name="Peter Maffay")
    # set the mongo database
    Singer.mongo_database = mongo

    s.reference["XXX"] = 10
    s.series["PRICE"] = ts
    assert s.reference.keys() == {"XXX"}
    return s


class TestProduct(object):
    def test_lt(self, mongo):
        assert Singer(name="A") < Singer(name="B")

    def test_name(self, singer):
        assert str(singer) == "Peter Maffay"

        # can't change the name of a product!
        with pytest.raises(AttributeError):
            singer.name = "AA"

    def test_last(self, singer, ts):
        assert singer.series.last(key="PRICE") == ts.last_valid_index()
        assert singer.series.last(key="OPEN") is None

    def test_timeseries(self, singer, ts):
        pdt.assert_series_equal(singer.series["PRICE"], ts)

    def test_reference(self, singer):
        assert singer.reference["AHAH"] is None
        assert singer.reference.get(item="NoNoNo", default=5) == 5
        assert singer.reference["XXX"] == 10
        assert singer.reference.keys() == {"XXX"}
        assert singer.reference.collection
        assert {k: v for k, v in singer.reference.items()} == {"XXX": 10}

    def test_keys(self, ts, mongo):
        s = Singer(name="BC")
        s.series.write(data=ts, key="Correlation", second="C")
        s.series.write(data=ts, key="Correlation", second="E")

        # You can loop over all Correlations though
        for k in s.series.keys(key="Correlation"):
            assert k["name"] == "BC"
            assert k["key"] == "Correlation"
            assert k["second"] in {"C", "E"}

        for k, v in s.series.items(key="Correlation"):
            assert k["name"] == "BC"
            assert k["key"] == "Correlation"
            assert k["second"] in {"C", "E"}
            pdt.assert_series_equal(v, ts)

        for k in s.series:
            assert k["name"] == "BC"
            assert k["key"] == "Correlation"
            assert k["second"] in {"C", "E"}

    def test_merge(self, singer):
        singer.series.merge(key="PRICE", data=pd.Series(index=[1,2], data=[20, 30]))
        pdt.assert_series_equal(singer.series["PRICE"], pd.Series(data=[100,20,30]))

    def test_pandas_frame(self, singer, ts):
        frame = Singer.pandas_frame(key="PRICE", products=[singer])
        pdt.assert_series_equal(frame[singer], ts, check_names=False)

    def test_write(self, ts, singer):
        # create a first series...
        singer.series.write(data=ts, key="Correlation", second="C")
        pdt.assert_series_equal(singer.series.get(item="Correlation"), ts)

        # ... and a second series
        singer.series.write(data=ts, key="Correlation", second="E")

        # you can't read a series named Correlation as there are two of them!
        with pytest.raises(AssertionError):
            pdt.assert_series_equal(singer.series.get(item="Correlation"), ts)

    def test_delete(self, singer):
        r1 = singer.series.delete()
        r2 = singer.reference.delete()
        assert r1.deleted_count == 1
        assert r2.deleted_count == 1
