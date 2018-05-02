import sqlalchemy as sq
from pyutil.sql.interfaces.products import ProductInterface


class Exchange(ProductInterface):
    name = sq.Column(sq.String(50), unique=True)
    exch_code = sq.Column(sq.String(50), unique=True)
    __mapper_args__ = {"polymorphic_identity": "exchange"}