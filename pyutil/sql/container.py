import pandas as pd

from pyutil.sql.immutable import ReadList
from pyutil.sql.models import PortfolioSQL, Symbol


class Assets(ReadList):
    def __init__(self, seq):
        super().__init__(seq, Symbol)

    @property
    def reference(self):
        return pd.DataFrame({asset.bloomberg_symbol: pd.Series(asset.reference) for asset in self}).transpose()

    @property
    def internal(self):
        return {asset.bloomberg_symbol: asset.internal for asset in self}

    @property
    def group(self):
        return {asset.bloomberg_symbol: asset.group.name for asset in self}

    @property
    def group_internal(self):
        return pd.DataFrame({"Group": pd.Series(self.group), "Internal": pd.Series(self.internal)})

    def history(self, field="PX_LAST"):
        return pd.DataFrame({asset.bloomberg_symbol: asset.timeseries[field] for asset in self})


class Portfolios(ReadList):
    def __init__(self, seq):
        super().__init__(seq, PortfolioSQL)

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