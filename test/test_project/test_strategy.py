import pytest

from pyutil.mongo.engine.symbol import Symbol, Group
from pyutil.portfolio.portfolio import similar
from pyutil.mongo.engine.strategy import Strategy

from test.config import test_portfolio, mongo, resource
import pandas.util.testing as pdt


@pytest.fixture()
def strategy():
    with open(resource("source.py"), "r") as f:
        s = Strategy(name="Peter", source=f.read(), active=True, type="wild")

        assert s.portfolio is None
        assert s.last_valid_index is None
        s.portfolio = test_portfolio()
        assert s.source
        return s


@pytest.fixture()
def db(mongo, strategy):
    with mongo as m:
        g = Group(name="Alternatives")
        g.save()

        for asset in strategy.portfolio.assets:
            s = Symbol(name=asset, group=g)
            s.save()

        strategy.save()
        yield m
        # You are now running the test...
        # once you exit this function m is disconnected


def test_assets(strategy):
    assert strategy.name == "Peter"
    assert strategy.assets == test_portfolio().assets
    assert strategy.last_valid_index == test_portfolio().prices.last_valid_index()
    assert similar(strategy.portfolio, test_portfolio())
    assert similar(strategy.configuration(reader=None).portfolio, test_portfolio())


def test_reference(db, strategy):
    with db as m:
        frame = Strategy.reference_frame(products=[strategy])
        assert frame["active"]["Peter"]
        assert frame["type"]["Peter"] == "wild"
        assert frame["source"]["Peter"]


def test_navs(db):
    with db as m:
        strategies = Strategy.products(names=["Peter"])
        frame = Strategy.navs(strategies=strategies)
        pdt.assert_series_equal(frame["Peter"], test_portfolio().nav.series, check_names=False)
