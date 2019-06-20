import pytest

from pyutil.portfolio.portfolio import similar
from test.config import test_portfolio, resource
from pyutil.sql.interfaces.symbols.strategy import Strategy, strategies


@pytest.fixture()
def strategy():
    with open(resource("source.py"), "r") as f:
        yield Strategy(name="Peter", source=f.read(), active=True)


class TestStrategy(object):
    def test_module(self, strategy):
        x = strategy.configuration(reader=None)
        assert similar(x.portfolio, test_portfolio())

    def test_run(self):
        folder = resource("strat")
        d = {name : source for name, source in strategies(folder)}
        assert set(d.keys()) == {"P1", "P2"}

    def test_mongo(self, strategy):
        config = strategy.configuration(reader=None)
        portfolio = config.portfolio
        strategy.portfolio = portfolio
        assert similar(portfolio, strategy.portfolio)

    def test_assets(self, strategy):
        assert strategy.assets == test_portfolio().assets