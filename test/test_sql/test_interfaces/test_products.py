from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.model.ref import Field, FieldType, DataType
from test.test_sql.product import Product


class TestProductInterface(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p1 = Product(name="A")
        cls.p2 = Product(name="B")

        cls.f1 = Field(name="x", type=FieldType.dynamic, result=DataType.integer)
        cls.f2 = Field(name="y", type=FieldType.dynamic, result=DataType.integer)

    def test_name(self):
        self.assertEqual(self.p1.name, "A")

    def test_name_invariant(self):
        with self.assertRaises(AttributeError):
            self.p1.name = "AA"

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
        self.assertEqual(self.p1.get_reference(field="z", default=5), 5)

        # assert False

    def test_with_unknown_ts(self):
        x = self.p1.get_timeseries(name="Never seen before")
        pdt.assert_series_equal(x, pd.Series({}))
