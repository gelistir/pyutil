import sqlalchemy as sq

from pyutil.sql.interfaces.futures.category import FuturesCategory
from pyutil.sql.interfaces.futures.contract import Contract
from pyutil.sql.interfaces.futures.exchange import Exchange
from pyutil.sql.interfaces.products import ProductInterface, Products
from sqlalchemy.orm import relationship


class Future(ProductInterface):
    internal = sq.Column(sq.String(200), unique=True)
    quandl = sq.Column(sq.String(200), nullable=True)
    #_category_id = sq.Column("category_id", sq.Integer, sq.ForeignKey(FuturesCategory.id))
    #_exchange_id = sq.Column("exchange_id", sq.Integer, sq.ForeignKey(Exchange.id))
    #category = relationship(FuturesCategory, uselist=False, backref="future", foreign_keys=[_category_id])
    #exchange = relationship(Exchange, uselist=False, backref="future", foreign_keys=[_exchange_id])

    contracts = relationship(Contract, back_populates="_future", foreign_keys=[Contract.id], order_by=Contract.notice)
    # todo: test the ordering
    __mapper_args__ = {"polymorphic_identity": "Future"}

    def __init__(self, name, quandl=None, internal=None, exchange=None, category=None):
        super().__init__(name)
        self.quandl = quandl
        self.internal = internal
        #self.exchange = exchange
        #self.category = category

    @property
    def max_notice(self):
        try:
            return max(contract.notice for contract in self.contracts)
        except ValueError:
            return None

    @property
    def figis(self):
        return [c.figi for c in self.contracts]


class Futures(Products):
    def __init__(self, futures):
        super().__init__(futures, cls=Future, attribute="name")
