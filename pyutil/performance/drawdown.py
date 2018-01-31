import numpy as np
import pandas as pd


def drawdown(price):
    """
    Compute the drawdown for a nav or price series

    :param price: the price series

    :return: the drawdown
    """
    assert isinstance(price, pd.Series)
    assert price.index.is_monotonic_increasing

    high_water_mark = np.empty(len(price.index))
    moving_max_value = 0
    for i, value in enumerate(price.values):
        assert value > 0
        moving_max_value = max(moving_max_value, value)
        high_water_mark[i] = moving_max_value

    return pd.Series(data=1.0 - (price.values / high_water_mark), index=price.index)


def drawdown_periods(price, eps=0):
    """
    Compute the length of drawdown periods

    :param price: the price series

    :return: Series with (t_i, n) = (last Day before drawdown, number of days in drawdown)
    """
    d = drawdown(price=price)

    # Drawdown days
    is_down = d > eps

    # first day of drawdowns
    is_first = ((~is_down).shift(1) * is_down)
    is_first.iloc[0] = is_down.iloc[0]
    is_first = is_first.apply(bool)

    # last day of drawdowns
    is_last = (is_down * (~is_down).shift(-1))
    is_last.iloc[-1] = is_down.iloc[-1]
    is_last = is_last.apply(bool)

    is_first = list(is_first.loc[is_first].index)
    is_last = list(is_last.loc[is_last].index)

    return pd.Series({start: end - start for start, end in zip(is_first, is_last)})


if __name__ == '__main__':
    series = pd.Series({})
    print("Drawdown of empty series")
    print(drawdown(series))
