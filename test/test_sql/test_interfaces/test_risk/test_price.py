from unittest import TestCase

import pandas as pd

from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.security import Security
import pandas.util.testing as pdt


class TestPrice(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.security = Security(name="A")
        cls.security.ppp = pd.Series([1,2,3])

        #cls.price = Price(security=cls.security)
        #cls.price.data = pd.Series([1,2,3])


    def test(self):
        pdt.assert_series_equal(self.security.ppp, pd.Series([1,2,3]))

        self.security.ppp = pd.Series([4,5,6])
        pdt.assert_series_equal(self.security.ppp, pd.Series([4,5,6]))


class TestVolatilitySecurity(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.security = Security(name="A")
        cls.currency = Currency(name="USD")
        cls.security.vola[cls.currency] = pd.Series([1,2,3])

    def test(self):
        x = self.security.vola[self.currency]
        pdt.assert_series_equal(x, pd.Series([1, 2, 3]))

