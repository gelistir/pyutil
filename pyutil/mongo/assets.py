import pandas as pd
import numpy as np

from pyutil.mongo.asset import Asset


class Assets(object):
    def __init__(self, assets=None):
        """
        Group of assets

        :param assets:
        """
        self.__asset = dict()

        if assets is not None:
            for asset in assets:
                self.add(asset)

    def __getitem__(self, item):
        """ get a particular asset """
        return self.__asset[item]

    def __len__(self):
        """ Number of assets """
        return len(self.__asset)

    def keys(self):
        """ Keys of those assets """
        return self.__asset.keys()

    def items(self):
        """ Items """
        for k in self.keys():
            yield (k, self[k])

    def frame(self, key="PX_LAST", default=np.NaN, t0=pd.Timestamp("2002-01-01")):
        """ Produce a frame by looping over all assets """
        return pd.DataFrame({name: asset.data(key, default) for name, asset in self.items()}).dropna(how="all", axis=0).truncate(before=t0)

    def __repr__(self):
        return str.join("\n", [str(self[asset]) for asset in self.keys()])

    @property
    def reference(self):
        """ reference data """
        return pd.DataFrame({name: asset.reference for name, asset in self.items()}).transpose()

    def add(self, asset):
        """ add an asset """
        assert isinstance(asset, Asset)
        self.__asset[asset.name] = asset

