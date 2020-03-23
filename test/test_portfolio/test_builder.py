import pandas as pd
import pytest

from pyutil.portfolio.portfolio import Portfolio, merge
from test.config import read


@pytest.fixture(scope="module")
def portfolio():
    return Portfolio(prices=read("price.csv", parse_dates=True, index_col=0),
                     weights=read("weight.csv", parse_dates=True, index_col=0))


def test_builder():
    prices = pd.DataFrame(columns=["B", "A"], index=[1, 2], data=100)

    portfolio = Portfolio(prices=prices)
    portfolio[1] = pd.Series({"A": 0.5, "B": 0.5})
    portfolio[2] = pd.Series({"A": 0.3, "B": 0.7})

    assert portfolio.prices["A"][2] == 100
    assert portfolio.asset_returns["A"][2] == 0.0
    assert portfolio.weights["A"][1] == 0.5
    assert portfolio.cash[2] == 0.0
    assert str(portfolio) == "Portfolio with assets: ['B', 'A']"


def test_rename(portfolio):
    p = portfolio.subportfolio(assets=["B", "A"]).rename(names={"A": "AA", "B": "BB"})
    assert str(p) == "Portfolio with assets: ['BB', 'AA']"


def test_forward():
    prices = pd.DataFrame(columns=["A", "B"], index=[1, 2, 3], data=[[100, 120], [110, 110], [130, 120]])

    portfolio = Portfolio(prices=prices)
    portfolio[1] = pd.Series({"A": 0.5, "B": 0.4})

    # forward the portfolio
    for t, p in portfolio.loop():
        portfolio = p

    assert portfolio.weights["A"][3] == pytest.approx(0.56521739130434789, 1e-5)


def test_empty():
    portfolio = Portfolio(prices=pd.DataFrame({}))
    assert portfolio.empty


def test_merge():
    prices1 = pd.DataFrame(columns=["B", "A"], index=[1, 2], data=100)

    portfolio1 = Portfolio(prices=prices1)
    portfolio1[1] = pd.Series({"A": 0.5, "B": 0.5})
    portfolio1[2] = pd.Series({"A": 0.3, "B": 0.7})

    prices2 = pd.DataFrame(columns=["C", "D"], index=[1, 2], data=200)

    portfolio2 = Portfolio(prices=prices2)
    portfolio2[1] = pd.Series({"C": 0.5, "D": 0.5})
    portfolio2[2] = pd.Series({"C": 0.3, "D": 0.7})

    portfolio = merge(portfolios=[0.5*portfolio1, 0.5*portfolio2], axis=1)

    assert portfolio.assets == ["A", "B", "C", "D"]

    prices3 = pd.DataFrame(columns=["A", "B"], index=[1, 2], data=200)

    portfolio3 = Portfolio(prices=prices3)
    portfolio3[1]=pd.Series({"A": 0.5, "B": 0.5})
    portfolio3[2]=pd.Series({"A": 0.3, "B": 0.7})

    with pytest.raises(ValueError):
        # overlapping columns!
        merge(portfolios=[portfolio1, portfolio3], axis=1)
