import warnings

import sqlalchemy as sq

import pandas as pd
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from pyutil.performance.summary import fromNav
from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.sql.base import Base
from pyutil.sql.interfaces.products import ProductInterface #, Timeseries
from pyutil.sql.interfaces.series import Series
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.timeseries.merge import merge


class Portfolio(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "portfolio"}

    def __init__(self, name):
        super().__init__(name)

    def upsert_influx(self, portfolio, symbols):
        assert isinstance(portfolio, _Portfolio)

        self.weights = merge(old=self.weights, new=portfolio.weights)
        self.prices = merge(old=self.prices, new=portfolio.prices)

        # recompute the entire portfolio!
        portfolio_new = self.portfolio_influx

        for asset in portfolio_new.assets:
            if symbols[asset] not in set(self.symbols):
                self.symbols.append(symbols[asset])

        return portfolio_new

    @property
    def last(self):
        if self.prices is not None:
            return self.prices.last_valid_index()
        else:
            return None

    @property
    def portfolio_influx(self):
        return _Portfolio(prices=self.prices, weights=self.weights)

    @property
    def nav(self):
        return self.portfolio_influx.nav


    @property
    def leverage(self):
        return self.portfolio_influx.leverage

    def sector(self, total=False):
        symbolmap = {s.name : s.group.name for s in self.symbols}
        return self.portfolio_influx.sector_weights(symbolmap=symbolmap, total=total)

    @property
    def state(self):
        def percentage(x):
            return "{0:.2f}%".format(float(100.0 * x)).replace("nan%", "")

        frame = self.portfolio_influx.state#.applymap(percentage)

        frame["group"] = pd.Series({s.name : s.group.name for s in self.symbols})
        frame["internal"] = pd.Series({s.name : s.internal for s in self.symbols})

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


class Price(Series):
    __tablename__ = "portfolio_prices"
    __mapper_args__ = {"polymorphic_identity": "price"}
    id = sq.Column(sq.ForeignKey('series.id'), primary_key=True)

    __portfolio_id = sq.Column("portfolio_id", sq.Integer, sq.ForeignKey(Portfolio.id), nullable=False)

    def __init__(self, data=None):
        self.data = data


Portfolio._prices = relationship(Price, uselist=False, backref="portfolio")
Portfolio.prices = association_proxy("_prices", "data", creator=lambda data: Price(data=data))


class Weight(Series):
    __tablename__ = "portfolio_weights"
    __mapper_args__ = {"polymorphic_identity": "price"}
    id = sq.Column(sq.ForeignKey('series.id'), primary_key=True)

    __portfolio_id = sq.Column("portfolio_id", sq.Integer, sq.ForeignKey(Portfolio.id), nullable=False)

    def __init__(self, data=None):
        self.data = data


Portfolio._weights = relationship(Weight, uselist=False, backref="portfolio")
Portfolio.weights = association_proxy("_weights", "data", creator=lambda data: Weight(data=data))
