import pandas as pd


def drawdown(price):
    """
    Compute the drawdown for a nav or price series

    :param price: the price series

    :return: the drawdown
    """
    assert isinstance(price, pd.Series)
    assert price.index.is_monotonic_increasing
    assert not (price < 0).any()

    return 1 - price / price.expanding(min_periods=1).max()


def drawdown_periods(price, eps=0):
    """
    Compute the length of drawdown periods

    :param price: the price series

    :return: Series with (t_i, n) = (last Day before drawdown, number of days in drawdown)
    """
    d = drawdown(price=price)

    # the first price can not be in drawdown
    #assert d.iloc[0] == 0

    # Drawdown days
    is_down = d > eps
    #assert not is_down.iloc[0]

    s = pd.Series(index=is_down.index[1:], data=[r for r in zip(is_down[:-1], is_down[1:])])

    # move from no-drawdown to drawdown
    start = list(s[s == (False, True)].index)

    # move from drawdown to drawdown
    end = list(s[s == (True, False)].index)

    # eventually append the very last day...
    if len(end) < len(start):
        # add a point to the series... value doesn't matter
        end.append(s.index[-1])

    return pd.Series({s: e-s for s, e in zip(start, end)})