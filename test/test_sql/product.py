from pyutil.sql.interfaces.products import ProductInterface


class Product(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Test-Product"}

    def __init__(self, name):
        super().__init__(name)