from sqlalchemy import String, Column, Integer, ForeignKey

from pyutil.sql.interfaces.products import ProductInterface


class Product(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Test-Product"}
    name = Column(String)
    id = Column("id", Integer, ForeignKey(ProductInterface.id), primary_key=True)

    def __repr__(self):
        return "({prod})".format(prod=self.name)