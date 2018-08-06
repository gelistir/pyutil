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

        for asset in portfolio.assets:
            self.symbols.append(symbols[asset])

        ww = portfolio.weights.stack().to_frame(name="Weight")
        pp = portfolio.prices.stack().to_frame(name="Price")

        ww.index.names = ["Date", "Asset"]
        pp.index.names = ["Date", "Asset"]
        xx = pd.concat([ww, pp], axis=1).reset_index().set_index("Date")

        Portfolio.client.write_points(xx, measurement="xxx2", tag_columns=["Asset"], field_columns=["Weight", "Price"], tags={"Portfolio": self.name}, batch_size=10000, time_precision="s")

        portfolio_new = self.portfolio_influx

        # todo: drop data first...
        # update the nav
        super()._ts_upsert(ts=portfolio_new.nav.dropna(), field="nav", measurement="nav")

        # update the leverage
        super()._ts_upsert(ts=portfolio_new.leverage.dropna(), field="leverage", measurement="leverage")

        return portfolio_new

    @property
    def portfolio_influx(self):
        p = super().client.read_frame(measurement="xxx2", field="Price", tags=["Asset"], conditions={"Portfolio": self.name})
        w = super().client.read_frame(measurement="xxx2", field="Weight", tags=["Asset"], conditions={"Portfolio": self.name})
        return _Portfolio(prices=p, weights=w)

    @property
    def nav(self):
        return fromNav(super()._ts(field="nav", measurement="nav"))

    @property
    def leverage(self):
        return super()._ts(field="leverage", measurement="leverage")

    @staticmethod
    def nav_all():
        return ProductInterface.client.read_frame(measurement="nav", field="nav", tags=["name"])

    @staticmethod
    def leverage_all():
        return ProductInterface.client.read_frame(measurement="leverage", field="leverage", tags=["name"])

    def sector(self, total=False):
        symbolmap = {s.name : s.group.name for s in self.symbols}
        return self.portfolio_influx.sector_weights(symbolmap=symbolmap, total=total)


class PortfolioSymbol(Base):
    __tablename__ = 'portfolio_symbol'
    portfolio_id = Column(Integer, ForeignKey('portfolio.id'), primary_key=True)
    symbol_id = Column(Integer, ForeignKey('symbol.id'), primary_key=True)

    symbol = relationship(Symbol)
    portfolio = relationship(Portfolio, backref="portfolio_symbol")

    def __init__(self, symbol=None, portfolio=None):
        self.symbol = symbol
        self.portfolio = portfolio

Portfolio.symbols = association_proxy("portfolio_symbol", "symbol")
