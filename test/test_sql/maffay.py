from pyutil.sql.base import Base
from pyutil.sql.product import Product


class Maffay(Product, Base):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
