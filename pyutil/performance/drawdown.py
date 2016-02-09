import numpy as np
import pandas as pd


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