import pandas as pd
import numpy as np

import sqlalchemy as sq
from sqlalchemy.ext.orderinglist import ordering_list

from pyutil.sql.interfaces.futures.category import FuturesCategory
from pyutil.sql.interfaces.futures.contract import Contract
from pyutil.sql.interfaces.futures.exchange import Exchange
from pyutil.sql.interfaces.products import ProductInterface
from sqlalchemy.orm import relationship


class Future(ProductInterface):
    internal = sq.Column(sq.String(200), unique=True)
    quandl = sq.Column(sq.String(200), nullable=True)
    _category_id = sq.Column("category_id", sq.Integer, sq.ForeignKey(FuturesCategory.id))
    _exchange_id = sq.Column("exchange_id", sq.Integer, sq.ForeignKey(Exchange.id))
    category = relationship(FuturesCategory, uselist=False, backref="future", foreign_keys=[_category_id])
    exchange = relationship(Exchange, uselist=False, backref="future", foreign_keys=[_exchange_id])
    fut_gen_month = sq.Column(sq.String(200), nullable=True)
    contracts = relationship("Contract", back_populates="_future", foreign_keys=[Contract._future_id], order_by=Contract.notice, collection_class=ordering_list("notice"))
    # todo: test the ordering
    __mapper_args__ = {"polymorphic_identity": "Future"}

    def __init__(self, name, fut_gen_month=None, quandl=None, internal=None, exchange=None, category=None):
        super().__init__(name)
        self.quandl = quandl
        self.internal = internal
        self.exchange = exchange
        self.category = category
        self.fut_gen_month = fut_gen_month

    @property
    def max_notice(self):
        try:
            return self.contracts[-1].notice
        except IndexError:
            return None

    @property
    def figis(self):
        return [c.figi for c in self.contracts]

    def roll_builder(self, offset_days=0, offset_months=0):
        # Returns a series date, Contract to roll into...

        m = dict()

        # enforce that the contracts are sorted
        contracts = sorted(self.contracts, key=lambda x: x.notice)

        # offsets
        for r_out, r_in in zip(contracts[:-1], contracts[1:]):
            m[r_out.notice - pd.offsets.DateOffset(days=offset_days, months=offset_months)] = r_in

        # make sure also the first contract is included
        m[pd.Timestamp("1900-01-01")] = contracts[0]

        return _Rollmap(pd.Series(m))


class _Rollmap(pd.Series):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def truncate(self, before=None, after=None, **kwargs):
        after = after or self.index[-1]
        before = before or self.index[0]

        assert before >= self.index[0]
        assert after <= self.index[-1]

        if before in self.index:
            return self.truncate(before=before, after=after)
        else:
            x = self
            # can be done better
            x.loc[before] = np.nan
            return x.sort_index().ffill().truncate(before=before, after=after)
