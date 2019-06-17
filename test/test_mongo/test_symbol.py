from pyutil.mongo.mongo import Collection, client
import pytest

from pyutil.mongo.xsymbols import prices
from test.config import read
import pandas.util.testing as pdt


@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)


@pytest.fixture(scope="module")
def collection(ts):
    db = client('test-mongo', 27017)['test-database']
    c = Collection(collection=db.test_collection)

    c.insert(p_obj=ts, kind="PX_LAST", name="Dampf")
    c.insert(p_obj=ts, kind="PX_LAST", name="Maffay")

    return c


class TestPrices(object):
    def test_find_one(self, ts, collection):
        frame = prices(collection=collection, kind="PX_LAST")
        pdt.assert_series_equal(ts, frame["Dampf"], check_names=False)
