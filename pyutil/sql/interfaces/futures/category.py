from sqlalchemy import ForeignKey, Column

from pyutil.sql.interfaces.products import ProductInterface


class FuturesCategory(ProductInterface):
    __tablename__ = "futurescategory"
    __mapper_args__ = {"polymorphic_identity": "Category"}
    id = Column(ForeignKey(ProductInterface.id), primary_key=True)


