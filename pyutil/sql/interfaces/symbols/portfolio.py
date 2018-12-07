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
    id = sq.Column(sq.ForeignKey(ProductInterface.id), primary_key=True)

    # define the price...
    _price = relationship(Series, uselist=False,
                           primaryjoin="and_(ProductInterface.id==Series.product1_id, Series.name=='price')")
    prices = association_proxy("_price", "data", creator=lambda data: Series(name="price", data=data))

    _weights = relationship(Series, uselist=False,
                            primaryjoin="and_(ProductInterface.id==Series.product1_id, Series.name=='weight')")
    weights = association_proxy("_weights", "data", creator=lambda data: Series(name="weight", data=data))


    def __init__(self, name):
        super().__init__(name)

    def upsert(self, portfolio, symbols):
        assert isinstance(portfolio, _Portfolio)

        self.weights = merge(old=self.weights, new=portfolio.weights)
        self.prices = merge(old=self.prices, new=portfolio.prices)

        # recompute the entire portfolio!
        portfolio_new = self.portfolio

        for asset in portfolio_new.assets:
            if symbols[asset] not in set(self.symbols):
                self.symbols.append(symbols[asset])

        return portfolio_new

    @property
    def last(self):
        return last_index(self.prices)


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
        symbolmap = {s.name : s.group.name for s in self.symbols}
        return self.portfolio.sector_weights(symbolmap=symbolmap, total=total)

    @property
    def state(self):
        def percentage(x):
            return "{0:.2f}%".format(float(100.0 * x)).replace("nan%", "")

        frame = self.portfolio.state#.applymap(percentage)

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
