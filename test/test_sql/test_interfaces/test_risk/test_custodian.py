import unittest

from pyutil.sql.interfaces.risk.custodian import Custodian, Currency
from pyutil.sql.interfaces.risk.owner import Owner


class TestOwner(unittest.TestCase):
    def test_custodian(self):
        o = Owner(name="Peter")
        o.custodian = Custodian(name="UBS")
        self.assertEqual(o.custodian, Custodian(name="UBS"))


class TestCurrency(unittest.TestCase):
    def test_currency(self):
        o = Owner(name="Peter")
        o.currency = Currency(name="CHF")
        self.assertEqual(o.currency, Currency(name="CHF"))

