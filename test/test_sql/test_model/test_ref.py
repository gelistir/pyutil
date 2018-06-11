from unittest import TestCase
from pyutil.sql.model.ref import Field, FieldType, DataType
from test.test_sql.product import Product


class TestReference(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p1 = Product(name="A")
        cls.p2 = Product(name="B")

        cls.f1 = Field(name="x", type=FieldType.dynamic, result=DataType.integer)

    def test_reference(self):
        self.assertEqual(self.f1.type, FieldType.dynamic)
        self.p1.reference[self.f1] = "100"
        self.assertEqual(self.p1.reference[self.f1], 100)

        self.p1.reference[self.f1] = "120"
        self.assertEqual(self.p1.reference[self.f1], 120)

    def test_datatype(self):
        x = DataType.integer
        self.assertEqual(x.value, "integer")
        self.assertEqual(x("120"), 120)

    def test_field(self):
        f1 = Field(name="Peter", type=FieldType.dynamic, result=DataType.string)
        self.assertEqual(str(f1), "(Peter)")

    def test_eq(self):
        f1 = Field(name="Peter", type=FieldType.dynamic, result=DataType.string)
        f2 = Field(name="Peter", type=FieldType.dynamic, result=DataType.string)
        self.assertEqual(f1, f2)
        self.assertEqual(hash(f1), hash(f2))
