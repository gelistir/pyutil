from pyutil.mongo.mongo import Collection, client
import pytest

from test.config import read
import pandas.util.testing as pdt


@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)


@pytest.fixture(scope="module")
def collection(ts):
    db = client('test-mongo', 27017)['test-database']
    c = Collection(collection=db.test_collection)

    c.insert(p_obj=ts, First="Hans", Last="Dampf")
    c.insert(p_obj=ts, First="Hans", Last="Maffay")

    return c


class TestMongo(object):
    def test_parse(self):
        assert not Collection.parse(None)

    def test_find_one(self, ts, collection):
        x = Collection.parse(collection.find_one(First="Hans", Last="Dampf"))
        pdt.assert_series_equal(ts, x)

        with pytest.raises(AssertionError):
            # there are two Hans
            collection.find_one(First="Hans")


        # there is no Peter
        collection.find_one(First="Peter") == None

    def test_assert_insert(self, ts, collection):
        with pytest.raises(AssertionError):
            collection.insert(p_obj=ts, First="Hans") # I can not update as this is not unique

    def test_find(self, collection):
        a = [x for x in collection.find(First="Hans")]
        assert len(a) == 2


