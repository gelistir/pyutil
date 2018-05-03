import sqlalchemy as sq
from pyutil.sql.interfaces.products import ProductInterface


class Exchange(ProductInterface):
    exch_code = sq.Column(sq.String(50), unique=True)
    __mapper_args__ = {"polymorphic_identity": "Exchange"}

    def __init__(self, name, exch_code=None):
        super().__init__(name)
        self.exch_code = exch_code
