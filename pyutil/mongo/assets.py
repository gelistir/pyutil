import pandas as pd

from pyutil.mongo.asset import Asset

class Assets(object):
    def __init__(self, assets):
        """
        Group of assets

        :param assets:
        """
        self.__asset = dict()

        for asset in assets:
            assert isinstance(asset, Asset), "asset is of type {0}".format(type(asset))
            self.__asset[asset.name] = asset

    def __getitem__(self, item):
        """ get a particular asset """
        #
        return self.__asset[item]

    def __len__(self):
        """ Number of assets """
        return len(self.__asset)

    @property
    def asset_names(self):
        """ Keys of those assets """
        return self.__asset.keys()

    def __repr__(self):
        return str.join("\n", [str(self[asset]) for asset in self.asset_names])

    @property
    def reference(self):
        """ reference data """
        return pd.DataFrame({asset.name: asset.reference for asset in self.__iter__()}).transpose()

    def __iter__(self):
        for k in self.__asset.keys():
            yield self[k]

    @property
    def history(self):
        x = pd.concat({asset.name: asset.time_series for asset in self.__iter__()}, axis=1)
        return x.swaplevel(axis=1)

    @property
    def reader(self):
        """
        Exposes a fct pointer, a strategy will get a function pointer rather than data.
        This way the strategy can read only the assets it needs
        :return:
        """
        return lambda name: self.__asset[name]

    # if you later want to set the weight you have to use this function!
    #def __setitem__(self, key, value):
    #    # value is a dataframe
    #    for asset_name in self.__asset.keys():
    #        self.__asset[asset_name][key] = value[asset_name]

