from sqlalchemy import String, Column

from pyutil.sql.interfaces.products import ProductInterface


class Product(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Test-Product"}
    name = Column(String)

    def __repr__(self):
        return "({prod})".format(prod=self.name)