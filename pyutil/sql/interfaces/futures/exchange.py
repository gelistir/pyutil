from pyutil.sql.interfaces.products import ProductInterface


class Exchange(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Exchange"}

