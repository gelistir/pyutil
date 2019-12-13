import numpy as np
import pandas as pd


# def volatility_adjust(prices, vola=32, min_periods=50, winsor=4.2, n=1) -> pd.Series:
#     assert winsor > 0
#     # check that all indices are increasing
#     assert prices.index.is_monotonic_increasing
#     # make sure all entries non-negative
#     assert not (prices <= 0).any()
#
#     # go into log space, now returns are just simple differences
#     prices = np.log(prices)
#
#     for i in range(n):
#         # compute the returns
#         returns = prices.diff()
#         # estimate the volatility
#         volatility = returns.ewm(com=vola, min_periods=min_periods).std(bias=False)
#         # compute new log prices
#         prices = (returns / volatility).clip(lower=-winsor, upper=winsor).cumsum()
#
#     return prices


def oscillator(price, a=32, b=96, min_periods=100) -> pd.Series:
    def __geom(q):
        return 1.0 / (1 - q)

    osc = price.ewm(span=2 * a - 1, min_periods=min_periods).mean() - price.ewm(span=2 * b - 1,
                                                                                min_periods=min_periods).mean()
    l_fast = 1.0 - 1.0 / a
    l_slow = 1.0 - 1.0 / b
    return osc / np.sqrt(__geom(l_fast ** 2) - 2.0 * __geom(l_slow * l_fast) + __geom(l_slow ** 2))


def trend_new(prices, fast=32, slow=96, vola=32, winsor=4.2, f=np.tanh) -> pd.Series:
    return f(oscillator(volatility_adjusted_prices(prices=prices, com=vola, winsor=winsor), fast, slow, 2 * fast))


def returns(prices):
    return prices.pct_change().dropna()


def volatility(prices, com=50, annualized=False):
    x = returns(prices=prices).ewm(com=com, min_periods=com).std(bias=False).dropna()
    if annualized:
        x = x * np.sqrt(__periods_per_year(x))
    return x


def __periods_per_year(data):
    x = pd.Series(data=data.index)
    m = x.diff().mean().total_seconds()
    return np.round(365 * 24 * 60 * 60 / m, decimals=0)


def volatility_adjusted_prices(prices, com=50, winsor=4.2) -> pd.Series:
    return (returns(prices) / volatility(prices, com=com)).clip(lower=-winsor, upper=winsor).cumsum().dropna()
