from sqlalchemy import ForeignKey, Column

from pyutil.sql.interfaces.products import ProductInterface


class Exchange(ProductInterface):
    __tablename__ = "exchange"
    __mapper_args__ = {"polymorphic_identity": "Exchange"}
    id = Column(ForeignKey(ProductInterface.id), primary_key=True)

