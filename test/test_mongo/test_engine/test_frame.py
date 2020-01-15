from pyutil.mongo.engine.frame import Frame
from pyutil.portfolio.portfolio import similar, Portfolio

from test.config import portfolio


def test_frame(portfolio):
    f = Frame(name="Portfolio")
    f.prices = portfolio.prices
    f.weights = portfolio.weights

    # You don't need to save the data...
    # f.save()

    x = Portfolio(prices=f.prices, weights=f.weights)
    assert similar(x, portfolio)
