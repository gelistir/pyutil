import unittest

from test.test_sql import init_influxdb

from pyutil.sql.interfaces.risk.custodian import Custodian
from pyutil.sql.interfaces.risk.owner import Owner


class TestOwner(unittest.TestCase):
    def test_custodian(self):
        init_influxdb()
        custodian = Custodian(name="UBS Geneva")
        o = Owner(name="Peter")
        o.custodian = custodian
        self.assertEqual(o.custodian, custodian)
