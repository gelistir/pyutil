from pyutil.mongo.engine.frame import Frame
from pyutil.portfolio.portfolio import similar, Portfolio

from test.config import portfolio


def test_frame(portfolio):
    f = Frame(name="Portfolio", prices=portfolio.prices, weights=portfolio.weights)
    x = Portfolio(prices=f.prices, weights=f.weights)
    assert similar(x, portfolio)
