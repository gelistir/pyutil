from unittest import TestCase

import pandas as pd
import numpy as np
import pandas.util.testing as pdt

from pyutil.sql.interfaces.products import Products
from pyutil.sql.model.ref import Field, FieldType, DataType
from test.test_sql.product import Product


class TestProductInterface(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p1 = Product(name="A")
        cls.p2 = Product(name="B")

        cls.f1 = Field(name="x", type=FieldType.dynamic, result=DataType.integer)
        cls.f2 = Field(name="y", type=FieldType.dynamic, result=DataType.integer)

    def test_name_invariant(self):
        with self.assertRaises(AttributeError):
            self.p1.name="AA"

    def test_timeseries(self):
        self.p1.upsert_ts(name="correlation", data={pd.Timestamp("12-11-1978"): 0.5}, secondary=self.p2)
        self.p1.upsert_ts(name="price", data={pd.Timestamp("12-11-1978"): 10.0})

        self.assertTrue("price" in self.p1._timeseries.keys())
        self.assertTrue(("correlation", self.p2) in self.p1._timeseries.keys())

        pdt.assert_series_equal(self.p1.get_timeseries("price"), pd.Series({pd.Timestamp("12-11-1978"): 10.0}))

        # test the frame
        self.assertTrue(self.p1.frame("price").empty)
        pdt.assert_frame_equal(self.p1.frame("correlation"),
                               pd.DataFrame(index=[pd.Timestamp("12-11-1978")], columns=[self.p2], data=[[0.5]]))

        self.assertTrue(self.p1.frame("NoNoNo").empty)

    def test_reference(self):
        self.p1.reference[self.f1] = "100"
        self.assertEqual(self.p1.reference[self.f1], 100)
        # self.assertEqual(self.p1.refdata[self.f1], 100)

        self.p1.reference[self.f1] = "120"
        self.assertEqual(self.p1.reference[self.f1], 120)
        self.assertFalse(self.f2 in self.p1.reference.keys())

        with self.assertRaises(KeyError):
            self.p1.reference[self.f2]

        self.p1.reference[self.f2] = "10"
        self.p1.reference[self.f2] = "11"

        self.assertTrue(self.f2 in self.p1.reference.keys())

        # self.assertDictEqual({self.f1: 120, self.f2: 11}, self.p1.reference)
        print(type(self.p1.reference))
        print(dict(self.p1.reference))
        self.assertDictEqual({self.f1: 120, self.f2: 11}, dict(self.p1.reference))

    def test_timeseries_created(self):
        with self.assertRaises(AttributeError):
            self.p1.timeseries["maffay"] = pd.Series({})

    def test_with_unknown_fields(self):
        f = Field(name="z", type=FieldType.dynamic, result=DataType.integer)
        self.assertEqual(self.p1.get_reference(field=f, default=5), 5)
        self.assertIsNone(self.p1.get_reference(field=f))
        # assert False

    def test_with_unknown_ts(self):
        x = self.p1.get_timeseries(name="Never seen before")
        pdt.assert_series_equal(x, pd.Series({}))


class TestProducts(TestCase):
    def test_products(self):
        p1 = Product(name="A")
        p2 = Product(name="B")

        x = Products(products=[p1, p2], cls=Product, attribute="name")
        self.assertDictEqual(x.to_dict(), {"A": p1, "B": p2})

        for product in x:
            self.assertIsInstance(product, Product)

        self.assertEqual(x["A"], p1)
        self.assertEqual(str(x), "A Test-Product(A)\nB Test-Product(B)")

    def test_reference(self):
        p1 = Product(name="A")
        p2 = Product(name="B")

        field = Field(name="Field 1", result=DataType.string)

        x = Products(products=[p1, p2], cls=Product, attribute="name")
        self.assertTrue(x.reference.empty)

        p1.reference[field] = "X"
        p2.reference[field] = "Y"

        pdt.assert_frame_equal(x.reference, pd.DataFrame(index=["A", "B"], columns=["Field 1"], data=[["X"], ["Y"]]),
                               check_names=False)

    def test_timeseries(self):
        p1 = Product(name="A")
        p2 = Product(name="B")

        x = Products(products=[p1, p2], cls=Product, attribute="name")

        self.assertTrue(x.history(field="price").empty)

        p1.upsert_ts(name="price", data={pd.Timestamp("2017-01-01"): 20.0})

        pdt.assert_frame_equal(x.history(field="price", rename=True), pd.DataFrame(index=pd.DatetimeIndex([pd.Timestamp("2017-01-01").date()]), columns=["A","B"], data=[[20.0, np.nan]]), check_names=False)

    def test_wrong_product(self):
        p1 = Product(name="A")
        p2 = Product(name="B")

        with self.assertRaises(AssertionError):
            Products(products=[p1, p2, 3], cls=Product, attribute="name")


