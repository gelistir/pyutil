import pandas as pd

from pyutil.mongo.asset import Asset

def from_csv(file, ref_file):
    frame = pd.read_csv(file, index_col=0, parse_dates=True, header=[0, 1])
    reference = pd.read_csv(ref_file, index_col=0)
    #for asset in frame.keys().levels[0]:
    return Assets([Asset(name=asset, data=frame[asset], **reference.ix[asset].to_dict()) for asset in frame.keys().levels[0]])

    #print(frame.keys().levels[0])
    #print(frame)

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
    def names(self):
        """ Keys of those assets """
        return self.__asset.keys()

    def __repr__(self):
        return str.join("\n", [str(self[asset]) for asset in self.names])

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

    def apply(self, f):
        ### apply a function f to each asset
        return Assets([Asset(name=asset.name, data=f(asset.time_series), **asset.reference.to_dict()) for asset in self])

    def to_csv(self, file, ref_file):
        # write time series data to a file
        pd.concat({asset.name: asset.time_series for asset in self}, axis=1).to_csv(file)

        # write reference data to a file
        self.reference.to_csv(ref_file)

    def sub(self, names):
        """
        Extract a subgroup of assets
        """
        return Assets([self[name] for name in names])

    def tail(self, n):
        # swap levels, assets first, time series name second
        data = self.history.tail(n).swaplevel(axis=1)
        return Assets([Asset(name=asset, data=data[asset], **self[asset].reference.to_dict()) for asset in data.keys().levels[0]])

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__asset == other.__asset
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

