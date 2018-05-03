import enum as _enum

import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship as _relationship
from sqlalchemy.types import Enum as _Enum

from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.portfolio.portfolio import Portfolio as _Portfolio


class StrategyType(_enum.Enum):
    mdt = 'mdt'
    conservative = 'conservative'
    balanced = 'balanced'
    dynamic = 'dynamic'


class Strategy(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "strategy"}
    active = sq.Column(sq.Boolean)
    source = sq.Column(sq.String)
    _portfolio_id = sq.Column("portfolio_id", sq.Integer, sq.ForeignKey("portfolio.id"), nullable=False)
    _portfolio = _relationship(Portfolio, uselist=False, back_populates="strategy", foreign_keys=[_portfolio_id], lazy="joined")
    type = sq.Column(_Enum(StrategyType))

    def __init__(self, name, active=True, source="", type=StrategyType.conservative):
        super().__init__(name)
        self._portfolio = Portfolio(name=self.name)
        self.active = active
        self.source = source
        self.type = type

    def __module(self):
        from types import ModuleType

        compiled = compile(self.source, '', 'exec')
        module = ModuleType("module")
        exec(compiled, module.__dict__)
        return module

    def configuration(self, reader=None):
        # Configuration only needs a reader to access the symbols...
        # Reader is a function taking the name of an asset as a parameter
        module = self.__module()
        config = module.Configuration(reader=reader)
        return config

    def upsert(self, portfolio, days=0, assets=None):

        assert isinstance(portfolio, _Portfolio)

        if not self._portfolio.empty:

            p = portfolio.truncate(before=self.portfolio.last_valid_index() - pd.DateOffset(days=days))
            self._portfolio.upsert_portfolio(portfolio=p, assets=assets)

        else:
            self._portfolio.upsert_portfolio(portfolio=portfolio, assets=assets)

        return self.portfolio
        # todo: make strategy a ProductInterface
        # todo: update nav right here...

    @property
    def portfolio(self):
        return self._portfolio.portfolio


Portfolio.strategy = _relationship("Strategy", uselist=False, back_populates="_portfolio", primaryjoin="Portfolio.id == Strategy._portfolio_id")


# strategy has to be defined in a file
# defining a class Configuration (accepting a reader)
