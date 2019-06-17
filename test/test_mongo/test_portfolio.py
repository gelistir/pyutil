from pyutil.mongo.mongo import Collection, client
import pytest

from pyutil.mongo.xportfolio import read_portfolio, write_portfolio
from test.config import test_portfolio
import pandas.util.testing as pdt


@pytest.fixture(scope="module")
def collection():
    db = client('test-mongo', 27017)['test-database']
    c = Collection(collection=db.test_collection)
    write_portfolio(collection=c, name="TEST", portfolio=test_portfolio())
    return c


class TestPrices(object):
    def test_find_one(self, collection):
        p = read_portfolio(collection=collection, name="TEST")
        pdt.assert_frame_equal(p.prices, test_portfolio().prices)
        pdt.assert_frame_equal(p.weights, test_portfolio().weights)

    def test_fine_one_false(self, collection):
        p = read_portfolio(collection=collection, name="UNKNOWN")
        assert p is None

