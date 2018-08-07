import unittest


from pyutil.sql.interfaces.risk.custodian import Custodian
from pyutil.sql.interfaces.risk.owner import Owner


class TestOwner(unittest.TestCase):
    def test_custodian(self):
        Owner.client.recreate(dbname="test")

        o = Owner(name="Peter")
        o.custodian = Custodian(name="UBS")
        self.assertEqual(o.custodian, Custodian(name="UBS"))
