import copy
import pandas as pd


class Asset(object):
    def __init__(self, name, data, group="", internal="", link="", **kwargs):
        """
        :param name: Name of the Asset
        :param data: DataFrame of time series data
        :param kwargs: any reference/static data
        """
        self.__data = pd.DataFrame(index=data.index)

        if isinstance(data, pd.Series):
            data = data.to_frame(name="PX_LAST")

        assert not data.index.has_duplicates, "Data Index has duplicates"
        assert data.index.is_monotonic_increasing, "Data Index is not increasing"

        self.__data = data

        self.__name = name
        self.__ref = copy.deepcopy(kwargs)
        self.__group = group
        self.__internal = internal
        self.__link = link

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

    @property
    def internal(self):
        return self.__internal

    @property
    def group(self):
        return self.__group

    def __repr__(self):
        return "Asset {0} with series {1} and reference {2}".format(self.name, list(self.time_series.keys()),
                                                                    sorted(self.__ref.items()))

    @property
    def link(self):
        return self.__link

