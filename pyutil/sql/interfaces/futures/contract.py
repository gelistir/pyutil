from datetime import date as _Date

from sqlalchemy import Date
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

import sqlalchemy as sq
import pandas as _pd

from pyutil.sql.interfaces.products import ProductInterface, Products


class Contract(ProductInterface):
    _future_id = sq.Column("future_id", sq.Integer, sq.ForeignKey("future.id"), nullable=False)
    _future = relationship("Future", foreign_keys=[_future_id], back_populates="contracts")
    __notice = sq.Column("notice", Date)
    __figi = sq.Column("figi", sq.String(200), unique=True)
    bloomberg_symbol = sq.Column(sq.String(200))
    fut_month_yr = sq.Column(sq.String(200))

    __mapper_args__ = {"polymorphic_identity": "Contract"}

    def alive(self, today=None):
        today = today or _pd.Timestamp("today").date()
        assert isinstance(today, _Date)
        return self.__notice > today

    def __lt__(self, other):
        return self.notice < other.notice

    def __init__(self, figi, notice, bloomberg_symbol=None, fut_month_yr=None):
        super().__init__(name=figi)

        # how can I check it's a Future? Not really, I would need to import the Future! But Future imports Contract...
        #assert future is not None, "The future can not be none"

        assert isinstance(notice, _Date)

        self.__figi = figi
        self.__notice = notice
        self.bloomberg_symbol = bloomberg_symbol
        self.fut_month_yr = fut_month_yr


    @hybrid_property
    def future(self):
        return self._future

    @hybrid_property
    def figi(self):
        return self.__figi

    @hybrid_property
    def notice(self):
        return self.__notice

    @property
    def quandl(self):
        return "{quandl}{month}{year}".format(quandl=self._future.quandl, month=self.month_x, year=self.year)

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


class Contracts(Products):
    def __init__(self, futures):
        super().__init__(futures, cls=Contract, attribute="name")
