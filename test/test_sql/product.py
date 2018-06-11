from pyutil.sql.interfaces.products import ProductInterface


class Product(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Test-Product"}
