from sqlalchemy import Date
from sqlalchemy.orm import relationship

import sqlalchemy as sq
import pandas as _pd

from pyutil.sql.interfaces.products import ProductInterface


class Contract(ProductInterface):
    _future_id = sq.Column("future_id", sq.Integer, sq.ForeignKey("future.id"))
    future = relationship("Future", foreign_keys=[_future_id], back_populates="contracts")
    notice = sq.Column(Date)
    figi = sq.Column(sq.String(200), unique=True)
    bloomberg_symbol = sq.Column(sq.String(200), unique=True)
    fut_month_yr = sq.Column(sq.String(200), unique=True)

    __mapper_args__ = {"polymorphic_identity": "Contract"}

    def alive(self, today=None):
        today = today or _pd.Timestamp("today").date()
        return self.notice > today

    def __repr__(self):
        return self.bloomberg_symbol

    @property
    def quandl(self):
        return "{quandl}{month}{year}".format(quandl=self.future.quandl, month=self.month_x, year=self.year)

    @property
    def month_x(self):
        months = _pd.Series(index=["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"],
                           data=["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"])

        return months[self.month_xyz]

    @property
    def month_xyz(self):
        return self.fut_month_yr[:3]

    @property
    def year(self):
        y = int(self.fut_month_yr[4:])
        if y < 50:
            return y + 2000
        else:
            return y + 1900
