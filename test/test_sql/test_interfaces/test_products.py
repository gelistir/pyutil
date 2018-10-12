from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.influx.client import test_client
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.model.ref import Field, FieldType, DataType
from test.test_sql.product import Product

t0 = pd.Timestamp("2010-05-14")
t1 = pd.Timestamp("2010-05-15")

class TestProductInterface(TestCase):
    @classmethod
    def setUpClass(cls):
        ProductInterface.client = test_client()

        #init_influxdb()
        cls.p1 = Product(name="A")
        cls.p2 = Product(name="B")

        cls.f1 = Field(name="x", type=FieldType.dynamic, result=DataType.integer)
        cls.f2 = Field(name="y", type=FieldType.dynamic, result=DataType.string)

    @classmethod
    def tearDownClass(cls):
        ProductInterface.client.close()

    def test_name(self):
        self.assertEqual(self.p1.name, "A")

    def test_name_invariant(self):
        # you can not change the name of a product!
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

        self.assertTrue(self.f1 < self.f2)

        self.assertTrue(self.f2 in self.p1.reference.keys())
        self.assertDictEqual({self.f1: 120, self.f2: "11"}, dict(self.p1.reference))

        pdt.assert_frame_equal(ProductInterface.reference_frame(products=[self.p1]).fillna(""), pd.DataFrame(index=[self.p1], columns=["x", "y"], data=[[120, "11"]]))


        print(ProductInterface.reference_frame(products=[self.p1, self.p2]))

        #assert False

    def test_with_unknown_fields(self):
        f = Field(name="z", type=FieldType.dynamic, result=DataType.integer)
        self.assertEqual(self.p1.get_reference(field=f, default=5), 5)
        self.assertIsNone(self.p1.get_reference(field=f))
        self.assertEqual(self.p1.get_reference(field="z", default=5), 5)

    # here we can also test the timeseries stuff
    def test_timeseries(self):
        self.assertIsNotNone(self.p1.client)

    def test_discriminator(self):
        self.assertEqual(self.p1.discriminator, "Test-Product")

    #def test_read_frame(self):
    #    self.p1._ts_upsert(ts=pd.Series({t0: 10.1, t1: 10.2}), field="PX_LAST", measurement="products")
    #    self.p2._ts_upsert(ts=pd.Series({t0: 11.1, t1: 11.2}), field="PX_LAST", measurement="products")

    #    x = ProductInterface.read_frame(field="PX_LAST", measurement="products")

    #    pdt.assert_frame_equal(x, pd.DataFrame(index=pd.DatetimeIndex([t0, t1], name="time"),
    #                                           columns=pd.Index(["A","B"], name="name"),
    #                                           data=[[10.1, 11.1],[10.2, 11.2]]))

    def test_empty_ts(self):
        p = Product(name="CCC")
        pdt.assert_series_equal(p._ts(measurement="wurst", field="PX_LAST"), pd.Series({}))
        self.assertIsNone(p._last(field="PX_LAST", measurement="wurst"))


    def test_hash(self):
        x = {self.p1, self.p2}
        assert self.p1 in x

