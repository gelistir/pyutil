import pandas as pd
import pytest

from pyutil.portfolio.portfolio import Portfolio, merge
from pyutil.timeseries.merge import last_index


def test_builder():
    prices = pd.DataFrame(columns=["B", "A"], index=[1, 2], data=100)

    portfolio = Portfolio(prices=prices)
    portfolio.weights.loc[1] = {"A": 0.5, "B": 0.5}
    portfolio.weights.loc[2] = {"A": 0.3, "B": 0.7}

    assert portfolio.prices["A"][2] == 100
    assert portfolio.asset_returns["A"][2] == 0.0
    assert portfolio.weights["A"][1] == 0.5
    assert portfolio.cash[2] == 0.0

    assert last_index(portfolio.prices) == 2
    assert str(portfolio) == "Portfolio with assets: ['B', 'A']"


def test_rename():
    prices = pd.DataFrame(columns=["B", "A"], index=[1, 2], data=100)
    portfolio = Portfolio(prices=prices)
    portfolio = portfolio.rename(names={"A": "AA", "B": "BB"})
    assert str(portfolio) == "Portfolio with assets: ['BB', 'AA']"


def test_forward():
    prices = pd.DataFrame(columns=["A", "B"], index=[1,2,3], data=[[100,120],[110, 110],[130,120]])

    portfolio = Portfolio(prices = prices)

    portfolio.weights.loc[1] = {"A": 0.5, "B": 0.4}

    # forward the weights from the previous state
    portfolio.forward(2)
    portfolio.forward(3)

    assert portfolio.weights["A"][3] == pytest.approx(0.56521739130434789, 1e-5)


def test_empty():
    portfolio = Portfolio(prices = pd.DataFrame({}))
    #self.assertIsNone(last_index(portfolio.prices))
    assert not last_index(portfolio.prices)
    assert portfolio.empty


def test_merge():
    prices1 = pd.DataFrame(columns=["B", "A"], index=[1, 2], data=100)

    portfolio1 = Portfolio(prices=prices1)
    portfolio1.weights.loc[1] = {"A": 0.5, "B": 0.5}
    portfolio1.weights.loc[2] = {"A": 0.3, "B": 0.7}

    prices2 = pd.DataFrame(columns=["C", "D"], index=[1, 2], data=200)

    portfolio2 = Portfolio(prices=prices2)
    portfolio2.weights.loc[1] = {"C": 0.5, "D": 0.5}
    portfolio2.weights.loc[2] = {"C": 0.3, "D": 0.7}

    portfolio = merge(portfolios=[portfolio1, portfolio2], axis=1)

    assert portfolio.assets == ["A","B","C","D"]

    prices3 = pd.DataFrame(columns=["A", "B"], index=[1, 2], data=200)

    portfolio3 = Portfolio(prices=prices3)
    portfolio3.weights.loc[1] = {"A": 0.5, "B": 0.5}
    portfolio3.weights.loc[2] = {"A": 0.3, "B": 0.7}

    with pytest.raises(ValueError):
        # overlapping columns!
        merge(portfolios=[portfolio1, portfolio3], axis=1)

