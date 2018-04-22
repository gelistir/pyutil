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


class Portfolios(ReadList):
    def __init__(self, seq):
        super().__init__(seq, Portfolio)

    @property
    def mtd(self):
        frame = pd.DataFrame({portfolio.name: portfolio.nav.mtd_series for portfolio in self}).sort_index(ascending=False)
        frame.index = [a.strftime("%b %d") for a in frame.index]
        frame = frame.transpose()
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    @property
    def ytd(self):
        frame = pd.DataFrame({portfolio.name: portfolio.nav.ytd_series for portfolio in self}).sort_index(ascending=False)
        frame.index = [a.strftime("%b") for a in frame.index]
        frame = frame.transpose()
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    def recent(self, n=15):
        frame = pd.DataFrame({portfolio.name: portfolio.nav.recent() for portfolio in self}).sort_index(ascending=False)
        frame.index = [a.strftime("%b %d") for a in frame.index]
        frame = frame.head(n)
        frame = frame.transpose()
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    @property
    def period_returns(self):
        frame = pd.DataFrame({portfolio.name: portfolio.nav.period_returns for portfolio in self}).sort_index(ascending=False)
        return frame.transpose()

    @property
    def performance(self):
        frame = pd.DataFrame({portfolio.name: portfolio.nav.summary() for portfolio in self}).sort_index(ascending=False)
        return frame.transpose()

    @property
    def sector(self):
        p = self[0]
        print(p.sector_tail)

        frame = pd.DataFrame({portfolio.name: portfolio.sector_tail for portfolio in self})
        return frame.transpose()


    @property
    def frames(self):
        return {"recent": self.recent(),
            "ytd": self.ytd,
            "mtd": self.mtd,
            "sector": self.sector,
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