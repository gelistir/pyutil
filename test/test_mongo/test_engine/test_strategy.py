from pyutil.mongo.engine.strategy import Strategy, strategies
from pyutil.portfolio.portfolio import  similar
import pandas.util.testing as pdt
from test.config import *


def test_strategy(portfolio):
    Strategy.objects.delete()

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


def test_strategies():
    folder = resource(name="strat")
    for name, source in strategies(folder=folder):
        assert name in {"P1", "P2"}