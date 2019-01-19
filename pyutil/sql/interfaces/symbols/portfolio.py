import os

import sqlalchemy as sq

import pandas as pd
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.sql.base import Base
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.series import Series
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.timeseries.merge import merge, last_index


class Portfolio(ProductInterface):
    __tablename__ = "portfolio"
    __mapper_args__ = {"polymorphic_identity": "portfolio"}
    __searchable__ = ["name"]
    id = sq.Column(sq.ForeignKey(ProductInterface.id), primary_key=True)

    # define the price...
    _price_rel = relationship(Series, uselist=False, primaryjoin = ProductInterface.join_series("price"))
    _prices = association_proxy("_price_rel", "data", creator=lambda data: Series(name="price", data=data))

    _weights_rel = relationship(Series, uselist=False, primaryjoin = ProductInterface.join_series("weight"))
    _weights = association_proxy("_weights_rel", "data", creator=lambda data: Series(name="weight", data=data))

    def __init__(self, name):
        super().__init__(name)

    def upsert(self, portfolio, symbols=None):
        assert isinstance(portfolio, _Portfolio)

        self._weights = merge(old=self._weights, new=portfolio.weights)
        self._prices = merge(old=self._prices, new=portfolio.prices)

        # recompute the entire portfolio!
        portfolio_new = self.portfolio

        symbols = symbols or {s.name : s for s in self.symbols}

        for asset in portfolio_new.assets:
            assert isinstance(asset, str)
            assert asset in symbols.keys(), "The asset {asset} is not known.".format(asset=asset)

            if symbols[asset] not in set(self.symbols):
                # append the symbol to the symbols of the portfolio
                self.symbols.append(symbols[asset])

        return self

    @property
    def last(self):
        return last_index(self._prices)

    @property
    def portfolio(self):
        return _Portfolio(prices=self.prices, weights=self.weights)

    @property
    def nav(self):
        return self.portfolio.nav

    @property
    def leverage(self):
        return self.portfolio.leverage

    def sector(self, total=False):
        symbolmap = {s.name : s.group.value for s in self.symbols}
        return self.portfolio.sector_weights(symbolmap=symbolmap, total=total)

    @property
    def state(self):
        def percentage(x):
            return "{0:.2f}%".format(float(100.0 * x)).replace("nan%", "")

        frame = self.portfolio.state

        frame["group"] = pd.Series({s.name: s.group.value for s in self.symbols})
        frame["internal"] = pd.Series({s.name: s.internal for s in self.symbols})

        sector_weights = frame.groupby(by="group")["Extrapolated"].sum()
        frame["Sector Weight"] = frame["group"].apply(lambda x: sector_weights[x])
        frame["Relative Sector"] = frame["Extrapolated"] / frame["Sector Weight"]
        frame.index.name = "Symbol"

        keys = set(frame.keys())
        keys.remove("group")
        keys.remove("internal")
        for k in keys:
            frame[k] = frame[k].apply(percentage)

        return frame

    @property
    def prices(self):
        return self._prices

    @property
    def weights(self):
        return self._weights
    #
    # def to_csv(self, folder=None):
    #     if folder:
    #         if not os.path.exits(folder):
    #             os.makedirs(folder)
    #         self.weights.to_csv(os.path.join(folder, "weights.csv"))
    #         self.prices.to_csv(os.path.join(folder, "prices.csv"))
    #         self.symbols


class PortfolioSymbol(Base):
    __tablename__ = 'portfolio_symbol'
    __searchable__ = ["symbol"]
    _portfolio_id = Column("portfolio_id", Integer, ForeignKey('portfolio.id', onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)
    _symbol_id = Column("symbol_id", Integer, ForeignKey('symbol.id', onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)

    symbol = relationship(Symbol, lazy="joined")
    portfolio = relationship(Portfolio, backref="portfolio_symbol", lazy="joined")

    def __init__(self, symbol=None, portfolio=None):
        self.symbol = symbol
        self.portfolio = portfolio


Portfolio.symbols = association_proxy("portfolio_symbol", "symbol")
