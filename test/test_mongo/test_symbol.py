from pyutil.mongo.mongo import Collection, client
import pytest

from pyutil.mongo.xsymbols import read_prices, read_price, write_price
from test.config import read
import pandas.util.testing as pdt


@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)


@pytest.fixture(scope="module")
def collection(ts):
    db = client('test-mongo', 27017)['test-database']
    c = Collection(collection=db.test_collection)

    write_price(collection=c, data=ts, name="Dampf")
    write_price(collection=c, data=ts, name="Maffay")

    return c


class TestPrices(object):
    def test_read_prces(self, ts, collection):
        frame = read_prices(collection=collection, kind="PX_LAST")
        pdt.assert_series_equal(ts, frame["Dampf"], check_names=False)

    def test_read_price(self, ts, collection):
        x = read_price(collection=collection, name="Dampf")
        pdt.assert_series_equal(ts, x, check_names=False)

    def test_read_price_bad(self, collection):
        x = read_price(collection=collection, name="NONONO")
        assert x is None
