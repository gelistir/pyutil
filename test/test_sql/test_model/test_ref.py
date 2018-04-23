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

        self.p1.upsert_ref(field=self.f1, value="100")
        self.assertEqual(self.p1.reference["x"], 100)

        self.p1.upsert_ref(field=self.f1, value="120")
        self.assertEqual(self.p1.reference["x"], 120)

        self.p2.upsert_ref(field=self.f1, value="110")


