import numpy as np


def __volatility_adjust(prices, com=32, min_periods=50):
    returns = prices.pct_change()
    volatility = returns.ewm(com=com, min_periods=min_periods).std(bias=False)
    return returns / volatility


def __adjprice(prices, vola=32, winsor=4.2, min_periods=50):
    # volatility adjusted returns
    return winsorize(__volatility_adjust(prices, vola, min_periods), winsor=winsor).cumsum()


def oscillator(price, a=32, b=96, min_periods=100):
    def __geom(q):
        return 1.0 / (1 - q)

    osc = price.ewm(span=2 * a - 1, min_periods=min_periods).mean() - price.ewm(span=2 * b - 1, min_periods=min_periods).mean()
    l_fast = 1.0 - 1.0 / a
    l_slow = 1.0 - 1.0 / b
    return osc / np.sqrt(__geom(l_fast**2) - 2.0 * __geom(l_slow * l_fast) + __geom(l_slow**2))


def trend(price, a=32, b=96, vola=32, winsor=4.2, min_periods=50):
    return np.tanh(oscillator(__adjprice(price, vola, winsor, min_periods), a, b, 2 * min_periods))


def winsorize(data, winsor=4.2):
    return data.apply(np.clip, a_min=-winsor, a_max=winsor)