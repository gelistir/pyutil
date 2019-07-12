import pandas as pd
import pytest

from pyutil.mongo.mongo import create_collection
from pyutil.portfolio.portfolio import similar
from pyutil.sql.interfaces.products import ProductInterface
from test.config import test_portfolio, resource
from pyutil.sql.interfaces.symbols.strategy import Strategy, strategies

import pandas.util.testing as pdt

@pytest.fixture()
def strategy():
    with open(resource("source.py"), "r") as f:
        yield Strategy(name="Peter", source=f.read(), active=True)


# point to a new mongo collection...
ProductInterface.__collection__ = create_collection()
ProductInterface.__collection_reference__ = create_collection()


class TestStrategy(object):
    def test_module(self, strategy):
        x = strategy.configuration(reader=None)
        assert similar(x.portfolio, test_portfolio())

    def test_run(self):
        folder = resource("strat")
        d = {name: source for name, source in strategies(folder)}
        assert set(d.keys()) == {"P1", "P2"}

    def test_mongo(self, strategy):
        config = strategy.configuration(reader=None)
        portfolio = config.portfolio
        strategy.portfolio = portfolio
        assert similar(portfolio, strategy.portfolio)

    def test_assets(self, strategy):
        assert strategy.assets == test_portfolio().assets

    def test_portfolio_none(self):
        strategy = Strategy(name="Peter Maffay")
        assert strategy.portfolio is None

    def test_reference_frame(self, strategy):
        frame = Strategy.reference_frame([strategy])
        framex = pd.DataFrame(index=[strategy], columns=["source", "type", "active"], data=[[strategy.source, strategy.type, strategy.active]])
        framex.index.name = "strategy"
        pdt.assert_frame_equal(frame, framex)

