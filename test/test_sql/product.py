from sqlalchemy import ForeignKey, Column

from pyutil.sql.interfaces.products import ProductInterface


class Product(ProductInterface):
    __tablename__ = "product"
    __mapper_args__ = {"polymorphic_identity": "Test-Product"}
    id = Column(ForeignKey(ProductInterface.id), primary_key=True)

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
