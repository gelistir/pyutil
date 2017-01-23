import pandas as pd
import copy

from pyutil.mongo.asset import Asset
from .abcArchive import Archive


class CsvArchive(Archive):
    """
    Mainly used for unit-testing. This is a very minimalistic Archive providing
    the same interface to access historic data as its siblings. Note that after initialization the underlying
    data remains immutable. You can only access copies of the original data. You may pay some performance penalty
    for it but we sacrifice some performance for safety here...
    """
    def __init__(self, symbols=None, **kwargs):
        if symbols is not None:
            self.__symbols = copy.deepcopy(symbols)
        else:
            self.__symbols = pd.DataFrame({})

        self.__data = kwargs

    def history(self, assets=None, name="PX_LAST"):
        if assets is not None:
            return self.__data[name][assets].copy()
        else:
            return self.__data[name].copy()

    def reference(self):
        return self.__symbols

    def keys(self):
        return self.__data.keys()

    def asset(self, name):
        if name in self.reference().index:
            return Asset(name=name, data=pd.DataFrame({key: data[name] for key, data in self.__data.items()}), **self.reference().ix[name].to_dict())
        else:
            return Asset(name=name, data=pd.DataFrame({key: data[name] for key, data in self.__data.items()}))