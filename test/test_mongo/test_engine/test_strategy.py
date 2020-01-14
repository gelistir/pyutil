import pytest

from pyutil.mongo.engine.strategy import Strategy
from test.config import mongo
from pyutil.portfolio.portfolio import Portfolio, similar
import pandas.util.testing as pdt
from test.config import read, resource



@pytest.fixture()
def portfolio():
    return Portfolio(prices=read("price.csv", parse_dates=True, index_col=0),
                     weights=read("weight.csv", parse_dates=True, index_col=0))

def test_strategy(mongo, portfolio):
    with mongo as m:
        s = Strategy(name="mdt", type="mdt", active=True, source="AAA")

        assert s.source == "AAA"
        assert s.type == "mdt"
        assert s.active

        assert s.portfolio is None
        assert s.last_valid_index is None
        s.save()

        frame = Strategy.reference_frame()
        assert frame.index.name == "strategy"

        s.portfolio = portfolio
        pdt.assert_frame_equal(s.portfolio.weights, portfolio.weights)
        pdt.assert_frame_equal(s.portfolio.prices, portfolio.prices)

        s.save()

        navs = Strategy.navs()
        print(navs["mdt"])
        print(portfolio.nav)

        assert not navs["mdt"].empty


def test_source(portfolio):
    with open(resource("source.py"), "r") as f:
        s = Strategy(name="Peter", source=f.read(), active=True, type="wild")

        assert s.portfolio is None
        assert s.last_valid_index is None
        assert s.source
        assert s.assets == portfolio.assets

def test_last_valid(portfolio):
    s = Strategy(name="Maffay", source="AAA", active=True, type="wild2")
    s.portfolio = portfolio
    assert s.last_valid_index == portfolio.prices.last_valid_index()
    assert similar(s.portfolio, portfolio)
