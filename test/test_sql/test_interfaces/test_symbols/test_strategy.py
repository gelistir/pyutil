import pandas as pd

import pandas.util.testing as pdt
from test.config import test_portfolio, resource

from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType

import pytest

@pytest.fixture
def strategy():
    portfolio = test_portfolio()
    symbols = {name: Symbol(name, group=SymbolType.fixed_income) for name in portfolio.assets}

    strategy = Strategy(name="Peter", source="Hans", active=True)
    strategy.upsert(portfolio=portfolio, symbols=symbols)

    return strategy


class TestStrategy(object):
    def test_upsert(self, strategy):
        assert strategy.last == pd.Timestamp("2015-04-22")


        # extract the portfolio
        p = strategy.portfolio
        pdt.assert_frame_equal(p.weights, test_portfolio().weights, check_names=False)
        pdt.assert_frame_equal(p.prices, test_portfolio().prices, check_names=False)

        # upsert the last 10 days
        strategy.upsert(portfolio=5 * test_portfolio().tail(10), days=10, symbols=None)

        # extract again the portfolio
        p = strategy.portfolio

        x = p.weights.tail(12).sum(axis=1)

        assert x["2015-04-08"] == pytest.approx(0.3050477980398519, 1e-5)
        assert x["2015-04-13"] == pytest.approx(1.486652, 1e-5)

        a = strategy.to_json()
        assert a["name"] == "Peter"

    #def test_csv(self, strategy):
    #    # extract csv streams
    #    prices, weights = strategy.to_csv()

    #    # read streams back and compare
    #    pdt.assert_frame_equal(pd.read_csv(io.StringIO(prices), index_col=0, parse_dates=True), strategy.portfolio.prices)
    #    pdt.assert_frame_equal(pd.read_csv(io.StringIO(weights), index_col=0, parse_dates=True), strategy.portfolio.weights)

    #    test_dir = tempfile.mkdtemp()
    #    strategy.to_csv(folder=test_dir)
    #    strategy.read_csv(folder=test_dir, symbols=strategy.symbols)


    def test_sector(self, strategy):
        print(strategy.sector())
        print(strategy.state)
        # todo: finish test
