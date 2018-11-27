from pyutil.sql.interfaces.products import ProductInterface


class Custodian(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Custodian"}

    def __init__(self, name):
        super().__init__(name=name)
