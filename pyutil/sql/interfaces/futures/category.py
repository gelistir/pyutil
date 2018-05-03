from pyutil.sql.interfaces.products import ProductInterface


class FuturesCategory(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Category"}

    def __init__(self, name):
        super().__init__(name)
