import unittest

import os
os.environ["influxdb_host"] = "test-influxdb"
os.environ["influxdb_db"] = "test"

from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.owner import Owner


class TestCurrency(unittest.TestCase):
    def test_currency(self):
        Owner.client.recreate(dbname="test")

        o = Owner(name="Peter")
        o.currency = Currency(name="CHF")
        self.assertEqual(o.currency, Currency(name="CHF"))
