from pyutil.mongo.engine.frame import Frame
from pyutil.portfolio.portfolio import similar, Portfolio

from test.config import test_portfolio


def test_frame():
    p = test_portfolio()

    f = Frame(name="Portfolio")
    f.prices = p.prices
    f.weights = p.weights

    # You don't need to save the data...
    # f.save()

    x = Portfolio(prices=f.prices, weights=f.weights)
    assert similar(x, test_portfolio())
