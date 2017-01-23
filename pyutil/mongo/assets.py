import pandas as pd
import numpy as np


def from_archive(archive, names):
    assert not len(names)==0, "Please specify a list of assets."
    return Assets([archive.asset(name) for name in names])


class Assets(object):
    def __init__(self, assets):
        self.__asset = {asset.name: asset for asset in assets}

    def __getitem__(self, item):
        return self.__asset[item]

    def __len__(self):
        return len(self.__asset)

    def keys(self):
        return self.__asset.keys()

    def items(self):
        for k in self.keys():
            yield (k, self[k])

    def frame(self, key="PX_LAST", default=np.NaN, t0=pd.Timestamp("2002-01-01")):
        return pd.DataFrame({name: asset.data(key, default) for name, asset in self.items()}).dropna(how="all", axis=0).truncate(before=t0)

    def __repr__(self):
        return str.join("\n", [str(self[asset]) for asset in self.keys()])

    @property
    def symbols(self):
        return pd.DataFrame({name: asset.reference for name, asset in self.items()}).transpose()