import unittest

from pyutil.influx.client_test import init_influxdb
from pyutil.sql.interfaces.risk.custodian import Custodian
from pyutil.sql.interfaces.risk.owner import Owner


class TestOwner(unittest.TestCase):
    def test_custodian(self):
        init_influxdb()

        o = Owner(name="Peter")
        o.custodian = Custodian(name="UBS")
        self.assertEqual(o.custodian, Custodian(name="UBS"))
