import numpy as np
import copy


class Asset(object):
    def __init__(self, name, data, **kwargs):
        self.__name = name
        self.__data = data

        assert not data.index.has_duplicates, "Data Index has duplicates"
        assert data.index.is_monotonic_increasing, "Data Index is not increasing"

        self.__ref = copy.deepcopy(kwargs)

    @property
    def name(self):
        return self.__name

    def data(self, key=None, default=np.NaN):
        if key:
            if key in self.__data.keys():
                return self.__data[key].dropna()
            else:
                return default
        else:
            return self.__data

    @property
    def reference(self):
        return self.__ref

    def __repr__(self):
        return "Asset {0} with series {1} and reference {2}".format(self.name, list(self.series_names()), self.__ref)

    def series_names(self):
        return self.__data.keys()

    def reference_names(self):
        return self.__ref.keys()

