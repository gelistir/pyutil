from pyutil.sql.base import Base
from pyutil.sql.ppp import Product


class Maffay(Product, Base):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
