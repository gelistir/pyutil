from pyutil.sql.interfaces.products import ProductInterface


class Currency(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Currency"}
