import warnings

import pandas as pd
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from pyutil.performance.summary import fromNav
from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.sql.base import Base
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.symbols.symbol import Symbol


class Portfolio(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "portfolio"}

    def __init__(self, name):
        super().__init__(name)

    @property
    def last(self):
        return super()._last(measurement="nav", field="nav")

    def upsert_influx(self, portfolio, symbols):
        assert isinstance(portfolio, _Portfolio)

        ww = portfolio.weights.stack().to_frame(name="Weight")
        pp = portfolio.prices.stack().to_frame(name="Price")

        ww.index.names = ["Date", "Asset"]
        pp.index.names = ["Date", "Asset"]

        # combine into frame Date, Asset, Weight Price...

        frame = pd.concat([ww, pp], axis=1).reset_index().set_index("Date")

        self._frame_upsert(frame=frame, field_columns=["Weight", "Price"], measurement="portfoliosx",
                           tag_columns=["Asset"], tags={"name": self.name})


        #Portfolio.client.write_points(xx, measurement="xxx2",
        #                              tag_columns=["Asset"], field_columns=["Weight", "Price"], tags={"name": self.name},
        #                              batch_size=10000, time_precision="s")

        # recompute the entire portfolio!
        portfolio_new = self.portfolio_influx

        # update the nav
        self._ts_upsert(ts=portfolio_new.nav.dropna(), field="nav", measurement="nav")

        # update the leverage
        self._ts_upsert(ts=portfolio_new.leverage.dropna(), field="leverage", measurement="leverage")

        for asset in portfolio_new.assets:
            self.symbols.append(symbols[asset])

        return portfolio_new

    @property
    def portfolio_influx(self):
        p = self._ts(measurement="portfoliosx", field="Price", tags=["Asset"]).unstack(level="Asset")
        w = self._ts(measurement="portfoliosx", field="Weight", tags=["Asset"]).unstack(level="Asset")

        return _Portfolio(prices=p, weights=w)

    @property
    def nav(self):
        return fromNav(self._ts(field="nav", measurement="nav"))

    @property
    def leverage(self):
        return self._ts(field="leverage", measurement="leverage")
    #
    # @staticmethod
    # def nav_all():
    #     warnings.warn("deprecated", DeprecationWarning)
    #     return ProductInterface.client.read_frame(measurement="nav", field="nav", tags=["name"])
    #
    # @staticmethod
    # def leverage_all():
    #     warnings.warn("deprecated", DeprecationWarning)
    #     return ProductInterface.client.read_frame(measurement="leverage", field="leverage", tags=["name"])

    def sector(self, total=False):
        symbolmap = {s.name : s.group.name for s in self.symbols}
        return self.portfolio_influx.sector_weights(symbolmap=symbolmap, total=total)

    @property
    def state(self):
        frame = self.portfolio_influx.state
        frame["group"] = pd.Series({s.name : s.group.name for s in self.symbols})
        frame["internal"] = pd.Series({s.name : s.internal for s in self.symbols})

        sector_weights = frame.groupby(by="group")["Extrapolated"].sum()
        frame["Sector Weight"] = frame["group"].apply(lambda x: sector_weights[x])
        frame["Relative Sector"] = 100 * frame["Extrapolated"] / frame["Sector Weight"]
        return frame


class PortfolioSymbol(Base):
    __tablename__ = 'portfolio_symbol'
    portfolio_id = Column(Integer, ForeignKey('portfolio.id'), primary_key=True)
    symbol_id = Column(Integer, ForeignKey('symbol.id'), primary_key=True)

    symbol = relationship(Symbol, lazy="joined")
    portfolio = relationship(Portfolio, backref="portfolio_symbol", lazy="joined")

    def __init__(self, symbol=None, portfolio=None):
        self.symbol = symbol
        self.portfolio = portfolio

Portfolio.symbols = association_proxy("portfolio_symbol", "symbol")
