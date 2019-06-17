from pyutil.mongo.mongo import Collection, client
import pytest

from pyutil.mongo.xportfolio import portfolio
from test.config import test_portfolio
import pandas.util.testing as pdt


@pytest.fixture(scope="module")
def collection():
    db = client('test-mongo', 27017)['test-database']
    c = Collection(collection=db.test_collection)
    p = test_portfolio()
    c.insert(p.weights, kind="WEIGHTS", name="TEST")
    c.insert(p.prices, kind="PRICES", name="TEST")
    return c


class TestPrices(object):
    def test_find_one(self, collection):
        p = portfolio(collection=collection, name="TEST")
        pdt.assert_frame_equal(p.prices, test_portfolio().prices)
        pdt.assert_frame_equal(p.weights, test_portfolio().weights)
