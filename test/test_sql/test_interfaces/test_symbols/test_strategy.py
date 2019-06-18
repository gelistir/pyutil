import pytest

from pyutil.mongo.mongo import client, Collection
from pyutil.portfolio.portfolio import similar
from test.config import test_portfolio, resource
from pyutil.sql.interfaces.symbols.strategy import Strategy, strategies

@pytest.fixture(scope="module")
def collection():
    db = client('test-mongo', 27017)['test-database']
    c = Collection(collection=db.test_collection)
    return c

class TestStrategy(object):
    def test_module(self):
        with open(resource("source.py"), "r") as f:
            strategy = Strategy(name="Peter", source=f.read(), active=True)
            x = strategy.configuration(reader=None)
            assert similar(x.portfolio, test_portfolio())

    def test_run(self):
        folder = resource("strat")
        d = {name : source for name, source in strategies(folder)}
        assert set(d.keys()) == set(["P1", "P2"])

    def test_mongo(self, collection):
        with open(resource("source.py"), "r") as f:
            # create a strategy
            strategy = Strategy(name="Peter", source=f.read(), active=True)
            config = strategy.configuration(reader=None)
            portfolio = config.portfolio
            strategy.write_portfolio(portfolio=portfolio, collection=collection)
            portfolio2 = strategy.read_portfolio(collection=collection)
            assert similar(portfolio, portfolio2)