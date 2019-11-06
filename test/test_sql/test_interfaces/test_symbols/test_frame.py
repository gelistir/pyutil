import pytest

from pyutil.portfolio.portfolio import similar, Portfolio
from pyutil.sql.interfaces.frame import Frame
from pyutil.sql.product import Product
from test.config import test_portfolio, mongo

@pytest.fixture()
def frame(mongo):
    assert mongo
    Product.mongo_database = mongo
    assert Frame.mongo_database
    assert Product.mongo_database

    f = Frame(name="Portfolio")
    f.series["Prices"] = test_portfolio().prices
    f.series["Weight"] = test_portfolio().weights
    return f


class TestFrame(object):
    def test_x(self, mongo):
        assert mongo
        Frame.mongo_database = mongo
        assert Frame.mongo_database
        #print(Product.mongo_database)
        #assert Product.mongo_database
        print(Frame.mongo_database)
        print(mongo)


    def test_module(self, frame):
        p = frame.series["Prices"]
        w = frame.series["Weight"]
        x = Portfolio(prices=p, weights=w)
        assert similar(x, test_portfolio())



