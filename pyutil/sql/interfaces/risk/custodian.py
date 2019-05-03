from pyutil.sql.interfaces.products import ProductInterface


class Custodian(ProductInterface):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)


class Currency(ProductInterface):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
