from sqlalchemy.orm import relationship as _relationship

from pyutil.performance.summary import fromNav
from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.sql.interfaces.products import ProductInterface, association_table
from pyutil.sql.interfaces.symbols.symbol import Symbol

_association_table = association_table(left="symbol", right="portfolio")

Symbol.portfolio = _relationship("Portfolio", secondary=_association_table, back_populates="_symbols")


class Portfolio(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "portfolio"}
    _symbols = _relationship(Symbol, secondary=_association_table, back_populates="portfolio", lazy="joined")

    def __init__(self, name):
        super().__init__(name)

    @property
    def empty(self):
        return self.frame(name="price").empty and self.frame(name="weight").empty

    @property
    def symbols(self):
        return self._symbols

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
        return fromNav(self.get_timeseries(name="nav"))

    @property
    def leverage(self):
        return self.timeseries["leverage"]

    def sector(self, total=False):
         map = {asset: asset.group.name for asset in self.symbols}
         return self.portfolio.sector_weights(symbolmap=map, total=total)

    def sector_tail(self, total=False):
         w = self.sector(total=total)
         return w.loc[w.index[-1]].rename(None)

    #def to_html_dict(self, **kwargs):
    #    return fromNav(ts=self.nav, adjust=False).to_dictionary(name=self.name, **kwargs)

