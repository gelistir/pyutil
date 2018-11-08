from unittest import TestCase

import pandas as pd
from pyutil.sql.base import Base

from pyutil.sql.interfaces.products import Timeseries
from pyutil.sql.session import postgresql_db_test
from test.config import test_portfolio

import numpy as np

import pandas.util.testing as pdt

from test.test_sql.product import Product


class TestTimeseries(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session, connection_str = postgresql_db_test(base=Base, echo=False)
        cls.p1 = Product(name="A")
        cls.p2 = Product(name="B")

    def test_basic(self):
        # create  new price series
        x = Timeseries(product=self.p1, name="price")

        self.assertEqual(x.name, "price")

        pdt.assert_series_equal(x.series, pd.Series({}))
        self.assertIsNone(x.last)

        # set the series...
        x.series = pd.Series({1: 100.0, 2: 120.0, 3: 110.0}, name="Wurst")
        pdt.assert_series_equal(x.series, pd.Series({1: 100.0, 2: 120.0, 3: 110.0}, name="Wurst"))
        self.assertEqual(x.last, 3)

        a = pd.Series({2: 140, 3: 150})

        x.series = Timeseries.merge(new=a, old=x.series)

        pdt.assert_series_equal(x.series, pd.Series({1: 100.0, 2: 140.0, 3: 150.0}))
        pdt.assert_series_equal(a, pd.Series({2: 140, 3: 150}))

    def test_timeseries(self):
        x = Timeseries(product=self.p1, name="nav")
        nav = test_portfolio().nav
        x.series = nav.apply(float)
        pdt.assert_series_equal(x.series, nav.series)

    def test_speed(self):
        x = Timeseries(product=self.p1, name="price2")

        a = np.random.randn(1000000)
        x.series = pd.Series(data=a)

        # go via memory! This is slow
        self.session.add(x)
        self.session.commit()

        x = self.session.query(Timeseries).filter(Timeseries.name == "price2").one()

    def test_backref(self):
        p = Product(name="B")
        self.session.add(p)
        self.session.commit()

        x = Timeseries(product=p, name="a")
        y = Timeseries(product=p, name="b")

        pdt.assert_frame_equal(pd.DataFrame(dict(p.ts)), pd.DataFrame({"a": x.series, "b": y.series}))

    def test_speed_2(self):
        x = Timeseries(product=self.p1, name="wurst")

        a = np.random.randn(100, 100)
        x.series = pd.DataFrame(data=a)

        pdt.assert_frame_equal(x.series, pd.DataFrame(data=a))

    def test_product_to_ts(self):
        p = Product(name="C")
        p.ts["Wurst"] = pd.Series(data=[1, 2, 3])
        pdt.assert_series_equal(p.ts["Wurst"], pd.Series(data=[1, 2, 3]))

    def test_index(self):
        p = Product(name="E")
        x = Timeseries(product=p, name="Maffay")
        x.series = pd.Series(data=[1, 2, 3])
        x.index = [4, 5, 6]
        pdt.assert_series_equal(x.series, pd.Series(index=[4, 5, 6], data=[1, 2, 3]))

    def test_date(self):
        p = Product(name="F")
        x = Timeseries(product=p, name="Maffay")
        t0 = pd.Timestamp("2010-10-20").date()
        t1 = pd.Timestamp("2010-10-21").date()

        x.series = pd.Series(index=[t0, t1], data=[1,2])
        pdt.assert_series_equal(x.series, pd.Series(index=[t0, t1], data=[1, 2]))

        x.index = [pd.Timestamp(a) for a in x.index]
        pdt.assert_series_equal(x.series, pd.Series(index=[pd.Timestamp(t0), pd.Timestamp(t1)],
                                                    data=[1, 2]))

    def test_create_ts(self):
        p = Product(name="F")
        ts = p.create_or_get_ts(field="wurst")
        assert isinstance(ts, Timeseries)
        self.assertIsNone(ts.last)

        p.ts["wurst"] = pd.Series(index=[0,1], data=[2,3])

        ts = p.create_or_get_ts(field="wurst")

        pdt.assert_series_equal(p.ts["wurst"], pd.Series(index=[0,1], data=[2,3]))
        pdt.assert_series_equal(ts.series, pd.Series(index=[0,1], data=[2,3]))
