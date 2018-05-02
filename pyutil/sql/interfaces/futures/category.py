import sqlalchemy as sq
from pyutil.sql.interfaces.products import ProductInterface


class FuturesCategory(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Category"}
    name = sq.Column("name", sq.String(50), unique=True)