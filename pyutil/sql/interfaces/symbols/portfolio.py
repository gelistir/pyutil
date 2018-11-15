import warnings

import pandas as pd
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from pyutil.performance.summary import fromNav
from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.sql.base import Base
from pyutil.sql.interfaces.products import ProductInterface, Timeseries
from pyutil.sql.interfaces.symbols.symbol import Symbol


class Portfolio(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "portfolio"}

    def __init__(self, name):
        super().__init__(name)

    #@property
    #def last(self):
    #    try:
    #        return self.ts["prices"].index[-1]
    #    except:
    #        return None

    def upsert_influx(self, portfolio, symbols):
        assert isinstance(portfolio, _Portfolio)

        self.ts["weights"] = Timeseries.merge(old=self.get_ts(field="weights"), new=portfolio.weights)
        self.ts["prices"] = Timeseries.merge(old=self.get_ts(field="prices"), new=portfolio.prices)

        # recompute the entire portfolio!
        portfolio_new = self.portfolio_influx

        self.ts["nav"] = portfolio_new.nav.dropna()
        self.ts["leverage"] = portfolio_new.leverage.dropna()

        for asset in portfolio_new.assets:
            if symbols[asset] not in set(self.symbols):
                self.symbols.append(symbols[asset])

        return portfolio_new

    @property
    def portfolio_influx(self):
        return _Portfolio(prices=self.ts["prices"], weights=self.ts["weights"])

    @property
    def nav(self):
        return self.ts["nav"]


    @property
    def leverage(self):
        return self.ts["leverage"]

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
        frame["Sector Weight"] = frame["group"].apply(lambda x: sector_weights[x])#.apply(percentage)
        frame["Relative Sector"] = frame["Extrapolated"] / frame["Sector Weight"]#.apply(percentage)
        frame.index.name = "Symbol"

        keys = set(frame.keys())
        keys.remove("group")
        keys.remove("internal")
        for k in keys:
            frame[k] = frame[k].apply(percentage)

        #frame["Sector Weight"] = frame["Sector Weight"].apply(percentage)
        #frame["Relative Sector"] = frame["Relative Sector"].apply(percentage)
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
