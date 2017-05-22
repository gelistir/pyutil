import pandas as pd

from pyutil.mongo.asset import Asset


def from_csv(file, ref_file):
    frame = pd.read_csv(file, index_col=0, parse_dates=True, header=[0, 1])
    reference = pd.read_csv(ref_file, index_col=0)

    def __reader(name):
        return Asset(name=name, data=frame[name], **reference.ix[name].to_dict())

    return Assets({asset: __reader(asset) for asset in frame.keys().levels[0]})


class Assets(dict):
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

    def to_csv(self, file, ref_file):
        # write time series data to a file
        pd.concat({asset.name: asset.time_series for asset in self}, axis=1).to_csv(file)

        # write reference data to a file
        self.reference.to_csv(ref_file)

    def sub(self, names):
        """
        Extract a subgroup of assets
        """
        return Assets({name: self[name] for name in names})

    def tail(self, n):
        # swap levels, assets first, time series name second
        data = self.history.tail(n).swaplevel(axis=1)
        return Assets({name : Asset(name=name, data=data[name], **self[name].reference.to_dict()) for name in self.keys()})

    @property
    def empty(self):
        return len(self) == 0

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
