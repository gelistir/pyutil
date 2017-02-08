import pandas as pd

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
                self.__add(asset)

    def __getitem__(self, item):
        """ get a particular asset """
        #
        return self.__asset[item]

    def __len__(self):
        """ Number of assets """
        return len(self.__asset)

    def asset_names(self):
        """ Keys of those assets """
        return self.__asset.keys()

    #def frame(self, names=None, key="PX_LAST", t0=pd.Timestamp("2002-01-01")):
    #    """ Produce a frame by looping over all assets """
    #    if names is not None:
    #        return pd.DataFrame({name: self[name].time_series[key] for name in names}).dropna(how="all", axis=0).truncate(before=t0)
    #    else:
    #        return pd.DataFrame({name: self[name].time_series[key] for name in self.asset_names()}).dropna(how="all", axis=0).truncate(before=t0)

    def __repr__(self):
        return str.join("\n", [str(self[asset]) for asset in self.asset_names()])

    @property
    def reference(self):
        """ reference data """
        return pd.DataFrame({asset.name: asset.reference for asset in self.__iter__()}).transpose()

    def __add(self, asset):
        """ add an asset """
        assert isinstance(asset, Asset), "asset is of type {0}".format(type(asset))
        self.__asset[asset.name] = asset

    def __iter__(self):
        for k in self.asset_names():
            yield self[k]

    def reader(self, name):
        return self[name]

    @property
    def history(self):
        x = pd.concat({asset: self[asset].time_series for asset in self.asset_names()}, axis=1)
        return x.swaplevel(axis=1)

    # if you later want to set the weight you have to use this function!
    def __setitem__(self, key, value):
        # value is a dataframe
        for asset_name in self.__asset.keys():
            self.__asset[asset_name][key] = value[asset_name]

