import sqlalchemy as sq
from sqlalchemy.ext.hybrid import hybrid_property

from pyutil.sql.interfaces.products import ProductInterface


class Currency(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Currency"}
    __name = sq.Column("name", sq.String(50), unique=True, nullable=False)

    def __init__(self, name):
        self.__name = name

    @hybrid_property
    def name(self):
        return self.__name

    def __repr__(self):
        return "({name})".format(name=self.name)

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name

    def __hash__(self):
        return hash(self.name)