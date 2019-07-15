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
    def reference_frame(products, f=lambda x: x):
        frame = Security._reference_frame(products=products, f=f)
        frame["fullname"] = pd.Series({f(s): s.fullname for s in products})
        return frame
