import pandas as pd
import pandas.util.testing as pdt
import pytest

from pyutil.mongo.mongo import create_collection


@pytest.fixture()
def ts1():
    return pd.Series(data=[100, 200], index=[0, 1])


@pytest.fixture()
def ts2():
    return pd.Series(data=[300, 300], index=[1, 2])


@pytest.fixture()
def col(ts1):
    collection = create_collection()
    # Note that we don't define a name here...
    collection.upsert(value=ts1, key="PX_LAST", First="Hans", Last="Dampf")
    collection.upsert(value=ts1, key="PX_LAST", First="Hans", Last="Maffay")
    return collection


@pytest.fixture()
def col_reference():
    collection = create_collection()
    collection.upsert(value=2.0, key="XXX", name="HANS")
    collection.upsert(value=3.0, key="XXX", name="PETER")
    return collection


class TestMongo(object):
    def test_find_one(self, col_reference):
        assert col_reference.find_one(name="HANS").data == 2.0
        assert col_reference.find_one(name="HANS").meta == {"key": "XXX", "name": "HANS"}

    def test_not_unique(self, col):
        # not unique
        with pytest.raises(AssertionError):
            # there are two Hans
            col.find_one(First="Hans")

    def test_no_find(self, col):
        # there is no Peter
        assert col.find_one(First="Peter") is None

    def test_assert_insert(self, col, ts1):
        # can not upsert Hans are there are two of them...
        with pytest.raises(AssertionError):
            col.upsert(value=ts1, key="PX_LAST", First="Hans")

        # check that there are indeed 2
        assert len([x for x in col.find(First="Hans")]) == 2

    def test_overwrite(self, col, ts2):
        col.upsert(value=ts2, key="PX_LAST", First="Hans", Last="Maffay")
        pdt.assert_series_equal(ts2, col.find_one(Last="Maffay").data)

