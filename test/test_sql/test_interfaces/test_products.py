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

    def test_timeseries(self):
        self.p1.upsert_ts(name="correlation", data={pd.Timestamp("12-11-1978"): 0.5}, secondary=self.p2)
        self.p1.upsert_ts(name="price", data={pd.Timestamp("12-11-1978"): 10.0})

        self.assertSetEqual(set(self.p1._timeseries.keys()), set(self.p1.timeseries.keys()))
        self.assertTrue("price" in self.p1.timeseries.keys())
        self.assertTrue(("correlation", self.p2) in self.p1.timeseries.keys())

        # test the frame
        self.assertTrue(self.p1.frame("price").empty)
        pdt.assert_frame_equal(self.p1.frame("correlation"), pd.DataFrame(index=[pd.Timestamp("12-11-1978")], columns=[self.p2], data=[[0.5]]))

        self.assertTrue(self.p1.frame("NoNoNo").empty)

    def test_reference(self):
        self.p1.upsert_ref(field=self.f1, value="100")
        self.assertEqual(self.p1.reference["x"], 100)

        self.p1.upsert_ref(field=self.f1, value="120")
        self.assertEqual(self.p1.reference["x"], 120)

        self.assertIsNone(self.p1.reference["NoNoNo"])
