import enum as _enum

import pandas as pd
import sqlalchemy as sq
from sqlalchemy.orm import relationship as _relationship
from sqlalchemy.types import Enum as _Enum

from pyutil.sql.interfaces.portfolio import Portfolio
from pyutil.sql.interfaces.products import Base


class StrategyType(_enum.Enum):
    mdt = 'mdt'
    conservative = 'conservative'
    balanced = 'balanced'
    dynamic = 'dynamic'


Portfolio._strategy_id = sq.Column("strategy_id", sq.Integer, sq.ForeignKey("strategiesapp_strategy.id"), nullable=True)
Portfolio.strategy = _relationship("Strategy", back_populates="_portfolio")


class Strategy(Base):
    __tablename__ = "strategiesapp_strategy"

    _id = sq.Column("id", sq.Integer, primary_key=True, autoincrement=True)
    name = sq.Column(sq.String(50), unique=True)
    active = sq.Column(sq.Boolean)
    source = sq.Column(sq.String)
    _portfolio = _relationship(Portfolio, uselist=False, back_populates="strategy")
    type = sq.Column(_Enum(StrategyType))

    def __init__(self, name, active=True, source=""):
        self.name = name
        self._portfolio = Portfolio(name=self.name, strategy=self)
        self.active = active
        self.source = source

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

    @property
    def assets(self):
        return self._portfolio.assets

    def upsert(self, portfolio, days=0, assets=None):
        if self._portfolio.last_valid:
            # this is tricky. as the portfolio object may not contain an index yet...
            last_valid = self._portfolio.last_valid
            # update the existing portfolio object, think about renaming upsert into update...
            self._portfolio.upsert(portfolio=portfolio.truncate(before=last_valid - pd.DateOffset(days=days)), assets=assets)
        else:
            self._portfolio.upsert(portfolio=portfolio, assets=assets)

        return self._portfolio.portfolio

    @property
    def portfolio(self):
        return self._portfolio.portfolio
