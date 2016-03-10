import numpy as np
import pandas as pd
from collections import OrderedDict


def drawdown(price):
    """
    Compute the drawdown for a nav or price series

    :param price: the price series

    :return: the drawdown
    """
    assert isinstance(price, pd.Series)
    high_water_mark = np.empty(len(price.index))
    moving_max_value = 0
    for i, value in enumerate(price.values):
        moving_max_value = max(moving_max_value, value)
        high_water_mark[i] = moving_max_value

    return pd.Series(data=1.0 - (price / high_water_mark), index=price.index)


def drawdown_periods(price):
    """
    Compute the length of drawdown periods

    :param price: the price series

    :return: Series with (t_i, n) = (last Day before drawdown, number of days in drawdown)
    """
    d = drawdown(price=price)
    dd = d.reset_index(drop=True)

    nodes = list(dd[dd == 0].index)
    nodes.append(dd.index[-1])

    # different Tuesday, Monday = 1 => 0 drawdown
    # Wednesday, Monday = 2 => 1
    x = pd.Series({d.index[x[0]]: x[1]-x[0]-1 for x in zip(nodes[:-1], nodes[1:])})

    return x[x > 0].sort_values()
