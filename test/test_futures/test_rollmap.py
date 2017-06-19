import pandas as pd
from unittest import TestCase
from pyutil.futures.rollmap import roll_builder


class Contract(object):
    def __init__(self, notice):
        assert isinstance(notice, pd.Timestamp)
        self.__notice = notice

    @property
    def notice(self):
        return self.__notice

    @property
    def future(self):
        return "future"

    def __str__(self):
        return str(self.__notice)

    def __repr__(self):
        return str(self.__notice)


class TestRollmap(TestCase):
    def testBuilder(self):
        c1 = Contract(pd.Timestamp("1980-01-01"))
        c2 = Contract(pd.Timestamp("1981-01-01"))
        c3 = Contract(pd.Timestamp("1982-01-01"))

        x = roll_builder(contracts=[c2, c1, c3], offset_days=2, offset_months=1)

        self.assertEquals(x.dates[0], pd.Timestamp("1900-01-01"))
        self.assertEquals(x.dates[1], pd.Timestamp("1979-11-29"))
        self.assertEquals(x.contracts[0], c1)
        self.assertEquals(x.contracts[1], c2)

        y = x.truncate(before=pd.Timestamp("1980-05-05"))
        self.assertEquals(y.dates[0], pd.Timestamp("1980-05-05"))
        self.assertEquals(y.contracts[0], c2)
