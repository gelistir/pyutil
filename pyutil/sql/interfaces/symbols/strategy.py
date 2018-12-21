import enum as _enum

import pandas as pd
import sqlalchemy as sq
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum as _Enum

from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.performance.summary import fromNav
from pyutil.sql.interfaces.symbols.symbol import Symbol


def module(source):
    from types import ModuleType

    compiled = compile(source, '', 'exec')
    mod = ModuleType("module")
    exec(compiled, mod.__dict__)
    return mod


class StrategyType(_enum.Enum):
    mdt = 'mdt'
    conservative = 'conservative'
    balanced = 'balanced'
    dynamic = 'dynamic'


class Strategy(ProductInterface):
    __tablename__ = "strategy"
    __mapper_args__ = {"polymorphic_identity": "strategy"}
    id = sq.Column(sq.ForeignKey(ProductInterface.id), primary_key=True)
    active = sq.Column(sq.Boolean)
    source = sq.Column(sq.String)
    _portfolio_id = sq.Column("portfolio_id", sq.Integer, sq.ForeignKey("portfolio.id"), nullable=False)
    _portfolio = relationship(Portfolio, uselist=False, foreign_keys=[_portfolio_id], lazy="joined")
    type = sq.Column(_Enum(StrategyType))

    def __init__(self, name, active=True, source="", type=StrategyType.conservative):
        super().__init__(name)
        self._portfolio = Portfolio(name=self.name)
        self.active = active
        self.source = source
        self.type = type

    def configuration(self, reader=None):
        # Configuration only needs a reader to access the symbols...
        # Reader is a function taking the name of an asset as a parameter
        return module(self.source).Configuration(reader=reader)

    def upsert(self, portfolio, symbols, days=0):
        assert isinstance(portfolio, _Portfolio)

        assert self._portfolio

        # find the last stamp of weights...
        last = self.last

        if not last:
            self._portfolio.upsert(portfolio=portfolio, symbols=symbols)
        else:
            # We only take the last few days of the new portfolio
            p1 = portfolio.truncate(before=last - pd.DateOffset(days=days))
            self._portfolio.upsert(portfolio=p1, symbols=symbols)

        return self.portfolio

    @property
    def portfolio(self):
        return self._portfolio.portfolio

    @property
    def last(self):
        return self._portfolio.last

    @property
    def assets(self):
        return self._portfolio.symbols

    @property
    def state(self):
        return pd.concat((self._portfolio.state, self.reference_assets), axis=1, sort=False)

    @property
    def reference_assets(self):
        return Symbol.frame(symbols=self.assets)#, name="Symbol")

    def to_json(self):
        nav = fromNav(self.portfolio.nav)
        return {"name": self.name, "Nav": nav, "Volatility": nav.ewm_volatility().dropna(), "Drawdown": nav.drawdown}

    def sector(self, total=False):
        return self._portfolio.sector(total=total)

    def to_csv(self, folder=None):
        return self.portfolio.to_csv(folder)

    def read_csv(self, folder, symbols):
        self.upsert(_Portfolio.read_csv(folder), symbols)
