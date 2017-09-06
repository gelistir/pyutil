import pandas as pd

from pyutil.mongo.asset import Asset


class Assets(object):
    def __init__(self, dict):
        self.__assets = dict

    def __getitem__(self, item):
        return self.__assets[item]

    def items(self):
        for name, asset in self.__assets.items():
            yield name, asset

    def keys(self):
        return self.__assets.keys()

    @property
    def empty(self):
        return self.len() == 0

    def len(self):
        return len(self.__assets)


    def __repr__(self):
        return str.join("\n", [str(self[asset]) for asset in self.keys()])

    @property
    def reference(self):
        """ reference data """
        return pd.DataFrame({name: asset.reference for name, asset in self.items()}).transpose()

    @property
    def history(self):
        return pd.concat({name: asset.time_series for name, asset in self.items()}, axis=1).swaplevel(axis=1)

    def apply(self, f):
        # apply a function f to each asset
        return Assets({name: Asset(name=name, data=f(asset.time_series), **asset.reference.to_dict()) for name, asset in self.items()})

    def sub(self, names):
        """
        Extract a subgroup of assets
        """
        return Assets({name: self[name] for name in names})

    def tail(self, n):
        # swap levels, assets first, time series name second
        data = self.history.tail(n).swaplevel(axis=1)
        return Assets({name : Asset(name=name, data=data[name], **self[name].reference.to_dict()) for name in self.keys()})

    def __eq__(self, other):
        return self.__assets == other.__assets

    @property
    def internal(self):
        return pd.Series({name: self[name].internal for name in self.keys()})

    @property
    def group(self):
        return pd.Series({name: self[name].group for name in self.keys()})