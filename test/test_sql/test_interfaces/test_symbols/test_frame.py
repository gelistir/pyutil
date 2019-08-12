import pytest

from pyutil.mongo.mongo import create_collection
from pyutil.portfolio.portfolio import similar, Portfolio
from pyutil.sql.interfaces.frame import Frame
from test.config import test_portfolio


@pytest.fixture()
def frame():
    f = Frame(name="Portfolio")
    f.series["Prices"] = test_portfolio().prices
    f.series["Weight"] = test_portfolio().weights
    return f


# point to a new mongo collection...
Frame.collection = create_collection()
Frame.collection_reference = create_collection()


class TestFrame(object):
    def test_module(self, frame):
        p = frame.series["Prices"]
        w = frame.series["Weight"]
        x = Portfolio(prices=p, weights=w)
        assert similar(x, test_portfolio())



