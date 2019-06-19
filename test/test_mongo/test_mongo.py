import pytest

import pandas as pd
import pandas.util.testing as pdt

from pyutil.mongo.mongo import collection as create_collection, _Collection, _mongo


@pytest.fixture()
def ts1():
    return pd.Series(data=[100, 200], index=[0, 1])


@pytest.fixture()
def ts2():
    return pd.Series(data=[300, 300], index=[1, 2])

@pytest.fixture()
def col(ts1):
    collection = create_collection(name="test")
    collection.upsert(p_obj=ts1, First="Hans", Last="Dampf")
    collection.upsert(p_obj=ts1, First="Hans", Last="Maffay")
    return collection

class TestMongo(object):
    def test_mongo(self):
        assert _mongo

    def test_parse_none(self):
        assert not _Collection.parse(None)

    def test_find_one(self, col, ts1):
        #collection.upsert(p_obj=ts1, First="Hans", Last="Dampf")
        #collection.upsert(p_obj=ts1, First="Hans", Last="Maffay")

        x = col.find_one(parse=True, First="Hans", Last="Dampf")
        pdt.assert_series_equal(ts1, x)

        with pytest.raises(AssertionError):
            # there are two Hans
            col.find_one(First="Hans")

        # there is no Peter
        assert col.find_one(First="Peter") is None
        assert col.find_one(parse=True, First="Peter") is None

    def test_assert_insert(self, col, ts1):
        #collection.upsert(p_obj=ts1, First="Hans", Last="Dampf")
        #collection.upsert(p_obj=ts1, First="Hans", Last="Maffay")
        with pytest.raises(AssertionError):
            col.upsert(p_obj=ts1, First="Hans")  # I can not update as this is not unique

    def test_find(self, col):
        #collection.upsert(p_obj=ts1, First="Hans", Last="Dampf")
        #collection.upsert(p_obj=ts1, First="Hans", Last="Maffay")
        a = [x for x in col.find(First="Hans")]
        assert len(a) == 2

    def test_name(self, col):
        assert col.name == "test"

    def test_merge(self, col, ts1, ts2):
        col.upsert(ts1, name="x")
        col.upsert(ts2, name="x")
        pdt.assert_series_equal(ts2, col.find_one(parse=True, name="x"))
