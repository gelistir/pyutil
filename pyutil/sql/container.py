import pandas as pd

from pyutil.sql.immutable import ReadList
from pyutil.sql.interfaces.portfolio import Portfolio
from pyutil.sql.interfaces.symbol import Symbol


class Assets(ReadList):
    def __init__(self, seq):
        super().__init__(seq, Symbol)

    @property
    def reference(self):
        x = pd.DataFrame({asset: pd.Series(asset.reference) for asset in self}).transpose()
        x.index.names = ["Product"]
        return x

    @property
    def internal(self):
        return {asset: asset.internal for asset in self}

    @property
    def group(self):
        return {asset: asset.group.name for asset in self}

    @property
    def group_internal(self):
        return pd.DataFrame({"Group": pd.Series(self.group), "Internal": pd.Series(self.internal)})

    def history(self, field="PX_LAST"):
        x = pd.DataFrame({asset: asset.timeseries[field] for asset in self})
        x.index.names = ["Date"]
        return x

    def to_dict(self):
        return {asset.bloomberg_symbol: asset for asset in self}

class Portfolios(ReadList):
    def __init__(self, seq):
        super().__init__(seq, Portfolio)
        self.__portfolio = {portfolio.name : portfolio.nav for portfolio in self}

    @property
    def mtd(self):
        frame = pd.DataFrame({key: item.mtd_series for key, item in self.__portfolio.items()}).sort_index(ascending=False)
        frame.index = [a.strftime("%b %d") for a in frame.index]
        frame = frame.transpose()
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    @property
    def ytd(self):
        frame = pd.DataFrame({key: item.ytd_series for key, item in self.__portfolio.items()}).sort_index(ascending=False)
        frame.index = [a.strftime("%b") for a in frame.index]
        frame = frame.transpose()
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    def recent(self, n=15):
        frame = pd.DataFrame({key: item.recent() for key, item in self.__portfolio.items()}).sort_index(ascending=False)
        frame.index = [a.strftime("%b %d") for a in frame.index]
        frame = frame.head(n)
        frame = frame.transpose()
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    @property
    def period_returns(self):
        frame = pd.DataFrame({key: item.period_returns for key, item in self.__portfolio.items()}).sort_index(ascending=False)
        return frame.transpose()

    @property
    def performance(self):
        frame = pd.DataFrame({key: item.summary() for key, item in self.__portfolio.items()}).sort_index(ascending=False)
        return frame.transpose()

    def sector(self, total=False):
        frame = pd.DataFrame({portfolio.name: portfolio.sector_tail(total=total) for portfolio in self})
        return frame.transpose()


    def frames(self, total=False):
        return {"recent": self.recent(),
            "ytd": self.ytd,
            "mtd": self.mtd,
            "sector": self.sector(total=total),
            "periods": self.period_returns,
            "performance": self.performance}


def state(portfolio):
    assert isinstance(portfolio, Portfolio)

    # this is now a list of proper symbol objects... portfolio is the database object!!!
    assets = Assets(portfolio.symbols)

    frame = pd.concat((assets.reference, portfolio.portfolio.state, assets.group_internal), axis=1, join="inner")
    frame = frame.rename(index=lambda x: x.bloomberg_symbol)

    sector_weights = frame.groupby(by="Group")["Extrapolated"].sum()
    frame["Sector Weight"] = frame["Group"].apply(lambda x: sector_weights[x])
    frame["Relative Sector"] = 100*frame["Extrapolated"] / frame["Sector Weight"]
    frame["Asset"] = frame.index

    return frame.set_index(["Group", "Sector Weight", "Asset"])