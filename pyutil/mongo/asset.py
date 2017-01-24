import numpy as np
import copy

import pandas as pd


class Asset(object):
    def __init__(self, name, data, **kwargs):
        """
        :param name: Name of the Asset
        :param data: DataFrame of time series data
        :param kwargs: any reference/static data
        """
        self.__name = name
        self.__data = data

        assert not data.index.has_duplicates, "Data Index has duplicates"
        assert data.index.is_monotonic_increasing, "Data Index is not increasing"

        self.__ref = copy.deepcopy(kwargs)

    @property
    def name(self):
        return self.__name

    def data(self, key=None, default=np.NaN):
        """
        Get a time series

        :param key:
        :param default:
        :return:
        """
        if key:
            # user specified a key
            if key in self.__data.keys():
                # return the series
                return self.__data[key].dropna()
            else:
                # if the key doesn't exist (think of "fx"), construct a constant series with a default value
                return pd.Series(index=self.__data.index, data=default)
        else:
            return self.__data

    @property
    def reference(self):
        return pd.Series(self.__ref)

    def __repr__(self):
        return "Asset {0} with series {1} and reference {2}".format(self.name, list(self.series_names()), sorted(self.__ref.items()))

    def series_names(self):
        return self.__data.keys()

    def reference_names(self):
        return self.__ref.keys()
