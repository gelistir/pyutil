import unittest

from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.owner import Owner


class TestCurrency(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ProductInterface.client.recreate(dbname="test")

    @classmethod
    def tearDownClass(cls):
        ProductInterface.client.close()

    def test_currency(self):
        o = Owner(name="Peter")
        o.currency = Currency(name="CHF")
        self.assertEqual(o.currency, Currency(name="CHF"))
