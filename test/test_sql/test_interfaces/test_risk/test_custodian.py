import unittest

from pyutil.sql.interfaces.risk.custodian import Custodian
from pyutil.sql.interfaces.risk.owner import Owner


class TestOwner(unittest.TestCase):
    def test_custodian(self):
        custodian = Custodian(name="UBS Geneva")
        o = Owner(name="Peter")
        o.custodian = custodian
        self.assertEqual(o.custodian, custodian)
