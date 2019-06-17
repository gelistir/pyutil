from pyutil.portfolio.portfolio import similar
from test.config import test_portfolio, resource
from pyutil.sql.interfaces.symbols.strategy import Strategy, strategies


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