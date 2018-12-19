from sqlalchemy import Column, ForeignKey, String

from pyutil.sql.interfaces.products import ProductInterface


class Custodian(ProductInterface):
    __tablename__ = "custodian"
    __mapper_args__ = {"polymorphic_identity": "Custodian"}
    id = Column(ForeignKey(ProductInterface.id, onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)
    #fullname = Column("fullname", String, nullable=True)

    def __init__(self, name):
        super().__init__(name=name)


class Currency(ProductInterface):
    __tablename__ = "currency"
    __mapper_args__ = {"polymorphic_identity": "Currency"}
    id = Column(ForeignKey(ProductInterface.id, onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)
    #fullname = Column("fullname", String, nullable=True)

    def __init__(self, name):
        super().__init__(name=name)


