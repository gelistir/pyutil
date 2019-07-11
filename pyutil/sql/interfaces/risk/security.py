import sqlalchemy as sq
from sqlalchemy.ext.hybrid import hybrid_property

from pyutil.sql.interfaces.products import ProductInterface


class Security(ProductInterface):
    fullname = sq.Column("fullname", sq.String, nullable=True)

    def __init__(self, name, fullname=None, **kwargs):
        super().__init__(name, **kwargs)
        self.fullname = fullname

    def __repr__(self):
        return "Security({id}: {name})".format(id=self.name, name=self["Name"])

    @hybrid_property
    def bloomberg_ticker(self):
        return self["Bloomberg Ticker"]

    @hybrid_property
    def bloomberg_scaling(self):
        return self["Bloomberg Multiplier"] or 1.0
