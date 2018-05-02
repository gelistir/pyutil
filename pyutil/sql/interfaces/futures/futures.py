import sqlalchemy as sq

from pyutil.sql.interfaces.futures.category import FuturesCategory
from pyutil.sql.interfaces.futures.contract import Contract
from pyutil.sql.interfaces.futures.exchange import Exchange
from pyutil.sql.interfaces.products import ProductInterface
from sqlalchemy.orm import relationship



class Futures(ProductInterface):

    name = sq.Column(sq.String(200), unique=True)
    internal = sq.Column(sq.String(200), unique=True)
    quandl = sq.Column(sq.String(200), nullable=True)
    _category_id = sq.Column("category_id", sq.Integer, sq.ForeignKey(FuturesCategory.id))
    _exchange_id = sq.Column("exchange_id", sq.Integer, sq.ForeignKey(Exchange.id))
    category = relationship(FuturesCategory, uselist=False, backref="futures", foreign_keys=[_category_id])
    exchange = relationship(Exchange, uselist=False, backref="futures", foreign_keys=[_exchange_id])

    contracts = relationship(Contract, back_populates="futures", foreign_keys=[Contract.id], order_by=Contract.notice)
    # todo: test the ordering
    __mapper_args__ = {"polymorphic_identity": "Future"}

    def __repr__(self):
        return self.name

    @property
    def max_notice(self):
        try:
            return max(contract.notice for contract in self.contracts)
        except ValueError:
            return None

    @property
    def figis(self):
        return [c.figi for c in self.contracts]