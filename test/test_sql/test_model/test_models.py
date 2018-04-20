from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.model.ref import Field, FieldType, DataType
from pyutil.sql.model.ts import Timeseries

from sqlalchemy import String, Column

from pyutil.sql.interfaces.products import ProductInterface



class _Product(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Test-Product"}
    name = Column(String, unique=True)

    def __repr__(self):
        return "({prod})".format(prod=self.name)


class TestTimeseries(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p1 = _Product(name="A")
        cls.p2 = _Product(name="B")

    def test_timeseries(self):
        ts1 = Timeseries(name="x", product=self.p1, data={2: 10.0}, secondary=self.p2)
        ts2 = Timeseries(name="y", product=self.p1, data={3: 11.0})
        ts3 = Timeseries(name="z", product=self.p1)

        self.assertIsNotNone(ts1.secondary)
        self.assertIsNone(ts2.secondary)

        pdt.assert_series_equal(ts2.series, pd.Series({3: 11.0}))
        pdt.assert_series_equal(ts1.series, pd.Series({2: 10.0}))

        self.assertEqual(ts1.key, ("x", self.p2))
        self.assertEqual(ts2.key, "y")

        pdt.assert_frame_equal(self.p1.frame("x"), pd.DataFrame(index=[2], columns=[self.p2], data=[[10.0]]))

        self.assertEqual(ts2.last_valid, 3)
        self.assertEqual(ts1.last_valid, 2)
        self.assertIsNone(ts3.last_valid)

        x = ts1.upsert(ts={2: 11.0, 3: 12.0})
        pdt.assert_series_equal(x.series, pd.Series({2: 11.0, 3: 12.0}))


class TestReference(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p1 = _Product(name="A")
        cls.p2 = _Product(name="B")

        cls.f1 = Field(name="x", type=FieldType.dynamic, result=DataType.integer)

    def test_reference(self):
        self.assertEqual(self.f1.type, FieldType.dynamic)

        self.p1.upsert_ref(field=self.f1, value="100")
        self.assertEqual(self.p1.reference["x"], 100)

        self.p1.upsert_ref(field=self.f1, value="120")
        self.assertEqual(self.p1.reference["x"], 120)


class TestProductInterface(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p1 = _Product(name="A")
        cls.p2 = _Product(name="B")

        cls.f1 = Field(name="x", type=FieldType.dynamic, result=DataType.integer)

    def test_timeseries(self):
        self.p1.upsert_ts(name="correlation", data={2: 0.5}, secondary=self.p2)
        self.p1.upsert_ts(name="price", data={2: 10.0})

        self.assertSetEqual(set(self.p1._timeseries.keys()), set(self.p1.timeseries.keys()))
        self.assertTrue("price" in self.p1.timeseries.keys())
        self.assertTrue(("correlation", self.p2) in self.p1.timeseries.keys())

        # test the frame
        self.assertTrue(self.p1.frame("price").empty)
        pdt.assert_frame_equal(self.p1.frame("correlation"), pd.DataFrame(index=[2], columns=[self.p2], data=[[0.5]]))

        self.assertTrue(self.p1.frame("NoNoNo").empty)

    def test_reference(self):
        self.p1.upsert_ref(field=self.f1, value="100")
        self.assertEqual(self.p1.reference["x"], 100)

        self.p1.upsert_ref(field=self.f1, value="120")
        self.assertEqual(self.p1.reference["x"], 120)

        self.assertIsNone(self.p1.reference["NoNoNo"])

