import pandas.util.testing as pdt
import pytest

from pyutil.portfolio.portfolio import similar
from pyutil.strategy.config import ConfigMaster
from test.config import portfolio, read_pd


@pytest.fixture()
def prices():
    return read_pd("price.csv", parse_dates=True, index_col=0)


@pytest.fixture()
def strategy(portfolio, prices):
    class Strategy(ConfigMaster):
        def __init__(self, names, reader, **kwargs):
            """
            :param names: the names of the assets used in the strategy
            :param reader: a function to access prices for the strategy
            :param kwargs:
            """
            super().__init__(names, reader, **kwargs)

        @property
        def portfolio(self):
            return portfolio.subportfolio(assets=self.names)

    return Strategy(names=["A", "B", "C"],  # Assets used by the strategy
                    reader=lambda name: prices[name],  # simple function to read prices
                    Hans=30, Dampf=40)  # parameter


def test_run_strategy(strategy):
    # you can always add new parameters or change existing ones
    strategy["Peter Maffay"] = 20
    strategy["Hans"] = 35
    assert strategy.parameter == {"Peter Maffay": 20, "Hans": 35, "Dampf": 40}


def test_prices(strategy, prices):
    pdt.assert_frame_equal(strategy.history(), prices[["A", "B", "C"]])


def test_names(strategy):
    assert strategy.names == ["A", "B", "C"]


def test_reader(strategy, prices):
    pdt.assert_series_equal(strategy.reader("A"), prices["A"])


def test_x(strategy, portfolio):
    assert similar(strategy.portfolio, portfolio.subportfolio(assets=strategy.names))
