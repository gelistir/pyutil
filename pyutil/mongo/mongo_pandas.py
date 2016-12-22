from builtins import AttributeError

import pandas as pd
import collections


def _flatten(d, parent_key=None, sep='.'):
    """ flatten dictonaries of dictionaries (of dictionaries of dict... you get it)"""
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(_flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def _mongo_series(x, format="%Y%m%d"):
    """ Convert a pandas time series into a dictionary (for Mongo)"""
    assert isinstance(x, pd.Series), "The argument is of type {0}. It has to be a Pandas Series".format(type(x))
    try:
        x.index = x.index.strftime(format)
    except AttributeError:
        pass

    return x.to_dict()


def _mongo_frame(x, format="%Y%m%d"):
    """ Convert a pandas DataFrame into a dictionary of dictionaries (for Mongo)"""
    assert isinstance(x, pd.DataFrame), "The argument is of type {0}. It has to be a Pandas DataFrame".format(type(x))
    try:
        x.index = x.index.strftime(format)
    except AttributeError:
        pass

    return {asset: _mongo_series(x[asset], format=format) for asset in x.keys()}

