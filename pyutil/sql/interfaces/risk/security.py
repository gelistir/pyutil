import pandas as pd
import sqlalchemy as sq

from pyutil.sql.interfaces.products import ProductInterface


class Security(ProductInterface):
    fullname = sq.Column("fullname", sq.String, nullable=True)

    def __init__(self, name, fullname=None, **kwargs):
        super().__init__(name, **kwargs)
        self.fullname = fullname

    def __repr__(self):
        return "Security({id}: {name})".format(id=self.name, name=self["Name"])

    @staticmethod
    def pandas_frame(products, key, **kwargs):
        return Security._pandas_frame(products=products, key=key, **kwargs)

    @staticmethod
    def reference_frame(products, f=lambda x: x):
        frame = Security._reference_frame(products=products)
        frame["fullname"] = pd.Series({s: s.fullname for s in products})
        frame.index = map(f, frame.index)
        frame.index.name = "security"
        return frame
