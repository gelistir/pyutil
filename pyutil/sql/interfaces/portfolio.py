import pandas as pd

import sqlalchemy as sq
from sqlalchemy.orm import relationship as _relationship

from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.sql.interfaces.products import ProductInterface, Base
from pyutil.sql.interfaces.symbol import Symbol

_association_table = sq.Table('association', Base.metadata,
                            sq.Column('symbol_id', sq.Integer, sq.ForeignKey('symbolsapp_symbol.id')),
                            sq.Column('portfolio_id', sq.Integer, sq.ForeignKey('portfolio2.id'))
                               )

Symbol.portfolio = _relationship("Portfolio", secondary=_association_table, back_populates="symbols")


class Portfolio(ProductInterface):
    __tablename__ = 'portfolio2'
    _id = sq.Column("id", sq.Integer, sq.ForeignKey(ProductInterface.id), primary_key=True)

    __mapper_args__ = {"polymorphic_identity": "portfolio"}
    symbols = _relationship(Symbol, secondary=_association_table, back_populates="portfolio")
    name = sq.Column(sq.String, unique=True)

    def upsert_price(self, symbol, data):
        assert isinstance(symbol, Symbol)
        if symbol not in self.symbols:
            self.symbols.append(symbol)
        self.upsert_ts(name="price", data=data, secondary=symbol)

    def upsert_weight(self, symbol, data):
        assert isinstance(symbol, Symbol)
        if symbol not in self.symbols:
            self.symbols.append(symbol)
        self.upsert_ts(name="weight", data=data, secondary=symbol)

    @property
    def empty(self):
        return self.frame(name="price").empty and self.frame(name="weight").empty

    def upsert(self, portfolio, assets=None):
        for symbol, data in portfolio.weights.items():
            if assets:
                symbol = assets[symbol]
            self.upsert_weight(symbol, data.dropna())

        for symbol, data in portfolio.prices.items():
            if assets:
                symbol = assets[symbol]
            self.upsert_price(symbol, data.dropna())

        return self

    @property
    def portfolio(self):
        # does it work?
        return _Portfolio(prices=self.price.sort_index(), weights=self.weight.sort_index())

    @property
    def last_valid(self):
        try:
            return self.portfolio.index[-1]
        except:
            return None

    @property
    def weight(self):
        return self.frame(name="weight").sort_index().rename(index=lambda x: pd.Timestamp(x))

    @property
    def price(self):
        return self.frame(name="price").sort_index().rename(index=lambda x: pd.Timestamp(x))

    @property
    def nav(self):
        return self.portfolio.nav

    def sector(self, total=False):
        map = {asset: asset.group.name for asset in self.symbols}
        return self.portfolio.sector_weights(symbolmap=map, total=total)

    def sector_tail(self, total=False):
        map = {asset: asset.group.name for asset in self.symbols}
        w = self.portfolio.sector_weights(symbolmap=map, total=total)
        return w.loc[w.index[-1]]

    @property
    def assets2(self):
        return self.portfolio.assets

    def __lt__(self, other):
        return self.name < other.name
