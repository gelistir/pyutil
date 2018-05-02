import sqlalchemy as sq
from sqlalchemy.ext.hybrid import hybrid_property

from pyutil.sql.interfaces.products import ProductInterface


class FuturesCategory(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Category"}
    __name = sq.Column("name", sq.String(50), unique=True)

    def __init__(self, name):
        self.__name = name

    @hybrid_property
    def name(self):
        return self.__name

    def __repr__(self):
        return "({name})".format(name=self.name)
