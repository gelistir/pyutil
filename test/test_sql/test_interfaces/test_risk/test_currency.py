import unittest

from pyutil.influx.client_test import init_influxdb
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.owner import Owner


class TestCurrency(unittest.TestCase):
    def test_currency(self):
        init_influxdb()

        o = Owner(name="Peter")
        o.currency = Currency(name="CHF")
        self.assertEqual(o.currency, Currency(name="CHF"))
