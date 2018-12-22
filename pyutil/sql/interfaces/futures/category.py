from sqlalchemy import ForeignKey, Column

from pyutil.sql.interfaces.products import ProductInterface


class FuturesCategory(ProductInterface):
    __tablename__ = "futurescategory"
    __mapper_args__ = {"polymorphic_identity": "Category"}
    id = Column(ForeignKey(ProductInterface.id, onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)


