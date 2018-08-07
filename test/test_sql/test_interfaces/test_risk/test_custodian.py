import unittest

from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.risk.custodian import Custodian
from pyutil.sql.interfaces.risk.owner import Owner


class TestOwner(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ProductInterface.client.recreate(dbname="test")

    @classmethod
    def tearDownClass(cls):
        ProductInterface.client.close()

    def test_custodian(self):
        o = Owner(name="Peter")
        o.custodian = Custodian(name="UBS")
        self.assertEqual(o.custodian, Custodian(name="UBS"))
