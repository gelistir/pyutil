import unittest

from pyutil.sql.interfaces.risk.custodian import Custodian, Currency
from pyutil.sql.interfaces.risk.owner import Owner


class TestOwner(object):
    def test_custodian(self):
        o = Owner(name="Peter")
        o.custodian = Custodian(name="UBS")
        assert o.custodian == Custodian(name="UBS")


class TestCurrency(object):
    def test_currency(self):
        o = Owner(name="Peter")
        o.currency = Currency(name="CHF")
        assert o.currency == Currency(name="CHF")
