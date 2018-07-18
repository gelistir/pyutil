from unittest import TestCase

from pyutil.influx.client_test import init_influxdb
from pyutil.sql.model.ref import Field, FieldType, DataType
from test.test_sql.product import Product

class TestProductInterface(TestCase):
    @classmethod
    def setUpClass(cls):
        init_influxdb()
        cls.p1 = Product(name="A")
        cls.p2 = Product(name="B")

        cls.f1 = Field(name="x", type=FieldType.dynamic, result=DataType.integer)
        cls.f2 = Field(name="y", type=FieldType.dynamic, result=DataType.string)

    def test_name(self):
        self.assertEqual(self.p1.name, "A")

    def test_name_invariant(self):
        with self.assertRaises(AttributeError):
            self.p1.name = "AA"

    def test_reference(self):
        self.p1.reference[self.f1] = "100"
        self.assertEqual(self.p1.reference[self.f1], 100)

        self.p1.reference[self.f1] = "120"
        self.assertEqual(self.p1.reference[self.f1], 120)
        self.assertFalse(self.f2 in self.p1.reference.keys())

        with self.assertRaises(KeyError):
            self.p1.reference[self.f2]

        self.p1.reference[self.f2] = "10"
        self.p1.reference[self.f2] = "11"

        self.assertTrue(self.f2 in self.p1.reference.keys())
        self.assertDictEqual({self.f1: 120, self.f2: "11"}, dict(self.p1.reference))

    def test_with_unknown_fields(self):
        f = Field(name="z", type=FieldType.dynamic, result=DataType.integer)
        self.assertEqual(self.p1.get_reference(field=f, default=5), 5)
        self.assertIsNone(self.p1.get_reference(field=f))
        self.assertEqual(self.p1.get_reference(field="z", default=5), 5)
