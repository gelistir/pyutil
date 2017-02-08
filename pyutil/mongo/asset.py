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
        """ Name of the asset. Immutable """
        return self.__name

    @property
    def time_series(self):
        return self.__data

    @property
    def reference(self):
        """
        Reference data for an asset. Given as a Pandas Series
        """
        return pd.Series(self.__ref).sort_index(axis=0)

    def __repr__(self):
        return "Asset {0} with series {1} and reference {2}".format(self.name, list(self.time_series.keys()), sorted(self.__ref.items()))

    def __setitem__(self, key, value):
        assert not value.index.has_duplicates, "Data Index has duplicates"
        assert value.index.is_monotonic_increasing, "Data Index is not increasing"
        self.__data[key] = value
