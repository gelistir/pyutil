import pandas as pd
import sqlalchemy as sq
from sqlalchemy import inspect
from sqlalchemy.orm import relationship as _relationship

from pyutil.performance.summary import fromNav
from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.symbol import Symbol, Symbols
from pyutil.sql.interfaces.association import association_table

_association_table = association_table(left="symbol", right="portfolio")

Symbol.portfolio = _relationship("Portfolio", secondary=_association_table, back_populates="symbols")


class Portfolio(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "portfolio"}
    symbols = _relationship(Symbol, secondary=_association_table, back_populates="portfolio", lazy="joined")
    name = sq.Column(sq.String, unique=True)

    @property
    def empty(self):
        return self.frame(name="price").empty and self.frame(name="weight").empty

    def upsert_portfolio(self, portfolio, assets=None):
        assert isinstance(portfolio, _Portfolio)
        for symbol, data in portfolio.weights.items():
            if assets:
                symbol = assets[symbol]

            if symbol not in self.symbols:
                self.symbols.append(symbol)

            self.upsert_ts(name="weight", secondary=symbol, data=data.dropna())

        for symbol, data in portfolio.prices.items():
            if assets:
                symbol = assets[symbol]
            self.upsert_ts(name="price", secondary=symbol, data=data.dropna())

        # recompute here the entire portfolio
        p = self.portfolio

        # upsert the underlying time series data, this is slow here but later when we access the data we don't need to recompute the nav or the leverage
        self.upsert_ts("nav", data=p.nav)
        self.upsert_ts("leverage", data=p.leverage)
        return self

    @property
    def portfolio(self):
        # does it work?
        return _Portfolio(prices=self.price, weights=self.weight)

    @property
    def weight(self):
        return self.frame(name="weight")

    @property
    def price(self):
        return self.frame(name="price")

    @property
    def nav(self):
        if "nav" in self.timeseries.keys():
            return fromNav(self.timeseries["nav"])
        else:
            return self.portfolio.nav

    @property
    def leverage(self):
        return self.timeseries["leverage"]

    def sector(self, total=False):
        map = {asset: asset.group.name for asset in self.symbols}
        return self.portfolio.sector_weights(symbolmap=map, total=total)

    def sector_tail(self, total=False):
        w = self.sector(total=total)
        return w.loc[w.index[-1]].rename(None)

    def __lt__(self, other):
        return self.name < other.name

    @property
    def state(self):

        # this is now a list of proper symbol objects... portfolio is the database object!!!
        assets = Symbols(self.symbols)

        frame = pd.concat((assets.reference, self.portfolio.state, assets.group_internal), axis=1,
                              join="inner")
        frame = frame.rename(index=lambda x: x.bloomberg_symbol)

        sector_weights = frame.groupby(by="Group")["Extrapolated"].sum()
        frame["Sector Weight"] = frame["Group"].apply(lambda x: sector_weights[x])
        frame["Relative Sector"] = 100 * frame["Extrapolated"] / frame["Sector Weight"]
        frame["Asset"] = frame.index

        return frame.set_index(["Group", "Sector Weight", "Asset"])


class Portfolios(list):
    def __init__(self, seq):
        super().__init__(seq)
        for a in seq:
            assert isinstance(a, Portfolio)
        self.__portfolio = {portfolio.name: portfolio.nav for portfolio in self}

    @property
    def mtd(self):
        frame = pd.DataFrame({key: item.mtd_series for key, item in self.__portfolio.items()}).sort_index(
            ascending=False)
        frame.index = [a.strftime("%b %d") for a in frame.index]
        frame = frame.transpose()
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    @property
    def ytd(self):
        frame = pd.DataFrame({key: item.ytd_series for key, item in self.__portfolio.items()}).sort_index(
            ascending=False)
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
        frame = pd.DataFrame({key: item.period_returns for key, item in self.__portfolio.items()}).sort_index(
            ascending=False)
        return frame.transpose()

    @property
    def performance(self):
        frame = pd.DataFrame({key: item.summary() for key, item in self.__portfolio.items()}).sort_index(
            ascending=False)
        return frame.transpose()

    def sector(self, total=False):
        for portfolio in self:
            print(type(inspect(portfolio)))
            print(inspect(portfolio).detached)
            print(inspect(portfolio).pending)


        frame = pd.DataFrame({portfolio.name: portfolio.sector_tail(total=total) for portfolio in self})
        return frame.transpose()

    def frames(self, total=False):
        return {"recent": self.recent(),
                "ytd": self.ytd,
                "mtd": self.mtd,
                "sector": self.sector(total=total),
                "periods": self.period_returns,
                "performance": self.performance}
