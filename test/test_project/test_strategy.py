import pytest

from pyutil.mongo.engine.symbol import Symbol, Group
from pyutil.portfolio.portfolio import similar
from pyutil.mongo.engine.strategy import Strategy

from test.config import test_portfolio, mongo_client, resource
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
def symbols(mongo_client):
    p = test_portfolio()
    g = Group(name="Alternatives")
    g.save()

    return [Symbol(name=a, group=g) for a in p.assets]


@pytest.fixture()
def db(mongo_client, strategy, symbols):
    for symbol in symbols:
        symbol.save()

    strategy.save()



class TestStrategy(object):
    # def test_symbol(self, db):
    #     assert db.connection
    #     assert db.session
    #
    #     # extract all strategies from database
    #     strategies = Strategy.products(session=db.session)
    #     #
    #     x = run(strategies=strategies, connection_str=db.connection, mongo_uri='mongodb://test-mongo:27017/test')
    #     assert x["Peter"]
    #     assert similar(x["Peter"], test_portfolio())

    def test_assets(self, strategy):
        assert strategy.name == "Peter"
        assert strategy.assets == test_portfolio().assets
        assert strategy.last_valid_index == test_portfolio().prices.last_valid_index()
        assert similar(strategy.portfolio, test_portfolio())
        assert similar(strategy.configuration(reader=None).portfolio, test_portfolio())

    def test_reference(self, db, strategy):
        frame = Strategy.reference_frame(products=[strategy])
        assert frame["active"]["Peter"]
        assert frame["type"]["Peter"] == "wild"
        assert frame["source"]["Peter"]

    def test_navs(self, db):
        strategies = Strategy.products(names=["Peter"])
        frame = Strategy.navs(strategies=strategies)
        pdt.assert_series_equal(frame["Peter"], test_portfolio().nav.series, check_names=False)
