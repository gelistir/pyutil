import pandas as pd
import sqlalchemy as sq

from pyutil.sql.interfaces.products import ProductInterface


class Security(ProductInterface):
    fullname = sq.Column("fullname", sq.String, nullable=True)

    def __init__(self, name, fullname=None, **kwargs):
        super().__init__(name, **kwargs)
        self.fullname = fullname

    def __repr__(self):
        return "Security({id}: {name})".format(id=self.name, name=self.reference["Name"])

    @staticmethod
    def reference_frame(securities, f=lambda x: x):
        frame = ProductInterface.reference_frame(products=securities, f=f)
        frame["fullname"] = pd.Series({f(s): s.fullname for s in securities})
        frame.index.name = "security"
        return frame
