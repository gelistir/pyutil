import pytest

import pandas as pd
import pandas.util.testing as pdt

from pyutil.mongo.mongo import collection as create_collection, _mongo


@pytest.fixture()
def ts1():
    return pd.Series(data=[100, 200], index=[0, 1])


@pytest.fixture()
def ts2():
    return pd.Series(data=[300, 300], index=[1, 2])


@pytest.fixture()
def col(ts1):
    collection = create_collection(write=True)
    collection.upsert(p_obj=ts1, First="Hans", Last="Dampf")
    collection.upsert(p_obj=ts1, First="Hans", Last="Maffay")
    # It is now forbidden to write into this collection
    collection.write = False
    return collection


class TestMongo(object):
    def test_mongo(self):
        assert _mongo

    def test_find_one(self, col, ts1):
        x = col.find_one(parse=True, First="Hans", Last="Dampf")
        pdt.assert_series_equal(ts1, x)

        with pytest.raises(AssertionError):
            # there are two Hans
            col.find_one(First="Hans")

        # there is no Peter
        assert col.find_one(parse=False, First="Peter") is None
        assert col.find_one(parse=True, First="Peter") is None

    def test_assert_insert(self, col, ts1):
        with pytest.raises(AssertionError):
            col.upsert(p_obj=ts1, First="Hans")  # I can not update as this is not unique

    def test_find(self, col):
        a = [x for x in col.find(First="Hans")]
        assert len(a) == 2

    def test_merge(self, ts1, ts2):
        col = create_collection()
        col.upsert(ts1, name="x")
        col.upsert(ts2, name="x")
        pdt.assert_series_equal(ts2, col.find_one(parse=True, name="x"))

    def test_upsert(self, ts1):
        col = create_collection()
        col.upsert(ts1, name="maffay", kind="wurst")
        y = col.find_one(parse=False, kind="wurst")
        assert y["name"] == "maffay"
        assert y["kind"] == "wurst"
        assert y["data"] == ts1.to_msgpack()

    def test_frame(self, col, ts1):
        frame = col.frame(key="Last")
        pdt.assert_frame_equal(frame, pd.DataFrame({"Maffay": ts1, "Dampf": ts1})[frame.keys()])

    def test_name(self, col):
        assert col.name
        # we just the same repr function in col...
        assert str(col) == str(col.collection)