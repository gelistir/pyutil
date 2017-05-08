import pandas as pd

from pyutil.mongo.asset import Asset


def from_csv(file, ref_file):
    frame = pd.read_csv(file, index_col=0, parse_dates=True, header=[0, 1])
    reference = pd.read_csv(ref_file, index_col=0)

    def __reader(name):
        return Asset(name=name, data=frame[name], **reference.ix[name].to_dict())

    return Assets([__reader(asset) for asset in frame.keys().levels[0]])


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
        return pd.DataFrame({asset.name: asset.reference for asset in self}).transpose()

    def __iter__(self):
        for k in self.__asset.keys():
            yield self[k]

    @property
    def history(self):
        x = pd.concat({asset.name: asset.time_series for asset in self}, axis=1)
        return x.swaplevel(axis=1)

    def apply(self, f):
        # apply a function f to each asset
        return Assets(
            [Asset(name=asset.name, data=f(asset.time_series), **asset.reference.to_dict()) for asset in self])

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
        return Assets(
            [Asset(name=asset, data=data[asset], **self[asset].reference.to_dict()) for asset in data.keys().levels[0]])

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__asset == other.__asset
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def empty(self):
        return len(self.__asset) == 0


    def reference_mapping(self, keys, mapd=None):
        mapd = mapd or Assets.map_dict()

        # extract the right reference data...
        refdata = self.reference[keys]

        # convert to datatypes
        for name in keys:
            if name in mapd:
                # convert the column if in the dict above
                refdata[[name]] = refdata[[name]].apply(mapd[name])

        return refdata

    @staticmethod
    def map_dict():
        map_dict = dict()
        map_dict["CHG_PCT_1D"] = lambda x: pd.to_numeric(x)
        map_dict["CHG_PCT_MTD"] = lambda x: pd.to_numeric(x)
        map_dict["CHG_PCT_YTD"] = lambda x: pd.to_numeric(x)
        map_dict["PX_LAST"] = lambda x: pd.to_numeric(x)
        map_dict["PX_CLOSE_DT"] = lambda x: pd.to_datetime(1e6 * x)
        map_dict["FUND_INCEPT_DT"] = lambda x: pd.to_datetime(1e6 * x)
        map_dict["PX_VOLUME"] = lambda x: pd.to_numeric(x)
        map_dict["VOLATILITY_20D"] = lambda x: pd.to_numeric(x)
        map_dict["VOLATILITY_260D"] = lambda x: pd.to_numeric(x)
        return map_dict
