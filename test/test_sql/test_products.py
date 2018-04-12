from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt
from sqlalchemy import Column, String

from pyutil.sql.common import DataType, FieldType
from pyutil.sql.products import ProductInterface, Timeseries, Field, ReferenceData


class Product(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "Test-Product"}
    name = Column(String, unique=True)


class TestProducts(TestCase):
    def test_ts1(self):
        a = Product(name="Peter Maffay")
        self.assertEqual(a.name, "Peter Maffay")

        ts = Timeseries(product=a, name="PX_LAST")
        print(ts)
        self.assertEqual(ts.name, "PX_LAST")
        self.assertEqual(ts.product, a)

        print(ts.series)
        ts.data[pd.Timestamp("12-11-1978").date()] = 22.0
        ts.data[pd.Timestamp("12-11-1978").date()] = 25.0
        print(ts.series)

    def test_ts2(self):
        a = Product(name="Peter Maffay")
        self.assertEqual(a.name, "Peter Maffay")
        a.timeseries["PX_LAST"] = pd.Series({pd.Timestamp("12-11-1978").date(): 25.0})
        print(a.timeseries)

    def test_field(self):
        f = Field(name="trades", result=DataType.integer, type=FieldType.dynamic)
        self.assertEqual(f.parse("100"), 100)

    def test_reference_data(self):
        f = Field(name="trades", result=DataType.integer, type=FieldType.dynamic)
        a = Product(name="Peter Maffay")
        d = ReferenceData(field=f, content="100", product=a)

        self.assertEqual(d.value, 100)

        a.refdata[f] = "200"
        self.assertEqual(a.refdata[f], 200)

    def test_field_1(self):
        f = Field(name="Field 1", type=FieldType.dynamic, result=DataType.string)

        self.assertEqual(str(f), "Field 1")
        self.assertEqual(f.type, FieldType.dynamic)
        self.assertEqual(f.result, DataType.string)

        self.assertEqual(f, Field(name="Field 1", type=FieldType.dynamic))
        s = Product(name="A")
        f.refdata[s] = "AHA"
        self.assertEqual(f.refdata[s], "AHA")
        pdt.assert_series_equal(f.reference, pd.Series({"A": "AHA"}))

        f.refdata[s] = "BAHA"
        self.assertEqual(f.refdata[s], "BAHA")
        pdt.assert_series_equal(f.reference, pd.Series({"A": "BAHA"}))

    def test_field_2(self):
        f = Field(name="Field 1", type=FieldType.dynamic, result=DataType.date)

        self.assertEqual(str(f), "Field 1")
        self.assertEqual(f.type, FieldType.dynamic)
        self.assertEqual(f.result, DataType.date)

        self.assertEqual(f, Field(name="Field 1", type=FieldType.dynamic))
        s = Product(name="A")
        f.refdata[s] = "1522886400000"
        self.assertEqual(f.refdata[s], pd.Timestamp("2018-04-05").date())
        pdt.assert_series_equal(f.reference, pd.Series({"A": pd.Timestamp("2018-04-05").date()}))

    def test_timeseries_of_symbol_1(self):
        s = Product(name="Peter Maffay")
        s.timeseries["Peter"] = pd.Series({1: 2.0, 5: 3.0})
        # this will return a pandas series
        t1 = s.timeseries["Peter"]
        # this will return the actual Timeseries class
        t2 = s._timeseries["Peter"]


        self.assertFalse(t1.empty)
        self.assertEqual(t1.last_valid_index(), 5)

        t2.upsert(ts=pd.Series({1: 7.0, 6: 3.0}))

        t1 = s.timeseries["Peter"]
        self.assertFalse(t1.empty)
        self.assertEqual(t1.last_valid_index(), 6)

        pdt.assert_series_equal(t1, pd.Series({1: 7.0, 5: 3.0, 6: 3.0}))

    def test_timeseries_of_symbol_2(self):
        s = Product(name="Peter Maffay")
        s.timeseries["Peter"] = pd.Series({1: 2.0, 5: 3.0})
        # that's a pandas Series
        ts = s.timeseries["Peter"]
        # that's an actual Timeseries sqlalchemy object
        s._timeseries["Peter"].data[5] = 4.0
        # back to pandas
        self.assertEqual(s.timeseries["Peter"][5], 4.0)

    def test_timeseries_of_symbol_3(self):
        s = Product(name="Peter Maffay")
        s.timeseries["Peter"] = pd.Series({1: 2.0, 5: 3.0})
        s.timeseries["Maffay"] = pd.Series({1: 2.0, 5: 5.0})
        self.assertEqual(s.frame["Maffay"][5], 5.0)

    def test_timeseries_of_symbol_4(self):
        s = Product(name="Peter Maffay")
        x = s.upsert_ts(key="Peter")
        x.upsert(pd.Series({1: 2.0, 5: 3.0}))
        self.assertEqual(s.timeseries["Peter"][5], 3.0)

    def test_last_valid(self):
        x = pd.Series({})
        self.assertIsNone(x.last_valid_index())

        s = Product(name="Peter Maffay")
        s.upsert_ts(key="Peter")
        self.assertIsNone(s.last_valid("Peter"))


