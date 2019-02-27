from sqlalchemy import ForeignKey, Column

from pyutil.sql.interfaces.products import ProductInterface


class Product(ProductInterface):
    __tablename__ = "product"
    __mapper_args__ = {"polymorphic_identity": "Test-Product"}

    # column is a ForeignKey to ProductInterfact table
    id = Column(ForeignKey(ProductInterface.id, onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)
