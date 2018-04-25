import sqlalchemy as sq
from sqlalchemy.orm import relationship as _relationship

from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.sql.interfaces.products import ProductInterface, Base
from pyutil.sql.interfaces.symbol import Symbol

_association_table = sq.Table('association', Base.metadata,
                              sq.Column('symbol_id', sq.Integer, sq.ForeignKey('symbol.id')),
                              sq.Column('portfolio_id', sq.Integer, sq.ForeignKey('portfolio.id'))
                              )

Symbol.portfolio = _relationship("Portfolio", secondary=_association_table, back_populates="symbols")


class Portfolio(ProductInterface):
    # the id property comes from HasIdMixin, so no longer needed...
    id = sq.Column("id", sq.Integer, sq.ForeignKey(ProductInterface.id), primary_key=True)

    __mapper_args__ = {"polymorphic_identity": "portfolio"}
    symbols = _relationship(Symbol, secondary=_association_table, back_populates="portfolio")
    name = sq.Column(sq.String, unique=True)

    @property
    def empty(self):
        return self.frame(name="price").empty and self.frame(name="weight").empty

    def upsert(self, portfolio, assets=None):
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
        return self.portfolio.nav

    def sector(self, total=False):
        map = {asset: asset.group.name for asset in self.symbols}
        return self.portfolio.sector_weights(symbolmap=map, total=total)

    def sector_tail(self, total=False):
        w = self.sector(total=total)
        return w.loc[w.index[-1]].rename(None)

    def __lt__(self, other):
        return self.name < other.name
