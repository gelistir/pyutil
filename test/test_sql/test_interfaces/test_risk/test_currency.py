import unittest

from test.test_sql import init_influxdb

from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.owner import Owner


class TestCurrency(unittest.TestCase):
    def test_currency(self):
        init_influxdb()

        o = Owner(name="Peter")
        currency = Currency(name="CHF")
        o.currency = currency
        self.assertEqual(o.currency, currency)
