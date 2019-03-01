import numpy as np
import warnings

def volatility_adjust(prices, vola=32, min_periods=50, winsor=4.2, n=1):
    assert winsor > 0
    # check that all indices are increasing
    assert prices.index.is_monotonic_increasing
    # make sure all entries non-negative
    assert not (prices <= 0).any()

    # go into log space, now returns are just simple differences
    prices = np.log(prices)

    for i in range(n):
        # compute the returns
        returns = prices.diff()
        # estimate the volatility
        volatility = returns.ewm(com=vola, min_periods=min_periods).std(bias=False)
        # compute new log prices
        prices = (returns / volatility).clip(lower=-winsor, upper=winsor).cumsum()

    return prices

#todo: Delete this function
def __volatility_adjust(prices, com=32, min_periods=50):
    warnings.warn("deprecated", DeprecationWarning)
    returns = prices.pct_change()
    volatility = returns.ewm(com=com, min_periods=min_periods).std(bias=False)
    return returns / volatility

#todo: Delete this function
def __winsorize(data, winsor=4.2):
    warnings.warn("deprecated", DeprecationWarning)
    return data.apply(np.clip, a_min=-winsor, a_max=winsor)


#todo: Delete this function
def adjprice(prices, vola=32, winsor=4.2, min_periods=50):
    warnings.warn("deprecated", DeprecationWarning)
    # volatility adjusted returns
    return __winsorize(__volatility_adjust(prices, vola, min_periods), winsor=winsor).cumsum()


def oscillator(price, a=32, b=96, min_periods=100):
    def __geom(q):
        return 1.0 / (1 - q)

    osc = price.ewm(span=2 * a - 1, min_periods=min_periods).mean() - price.ewm(span=2 * b - 1, min_periods=min_periods).mean()
    l_fast = 1.0 - 1.0 / a
    l_slow = 1.0 - 1.0 / b
    return osc / np.sqrt(__geom(l_fast**2) - 2.0 * __geom(l_slow * l_fast) + __geom(l_slow**2))

#todo: Delete this function
def trend(price, a=32, b=96, vola=32, winsor=4.2, min_periods=50, f=np.tanh):
    warnings.warn("deprecated", DeprecationWarning)
    return f(oscillator(adjprice(price, vola, winsor, min_periods), a, b, 2 * min_periods))


def trend_new(price, a=32, b=96, vola=32, winsor=4.2, min_periods=50, f=np.tanh, n=1):
    return f(oscillator(volatility_adjust(price,vola,min_periods,winsor,n),a,b,2*min_periods))
