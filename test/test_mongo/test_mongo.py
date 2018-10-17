from unittest import TestCase

import pandas as pd
import numpy as np

from mongoengine import connect
from mongoengine.errors import NotUniqueError

from pyutil.mongo.timeseries_old import Price
import pandas.util.testing as pdt

from test.config import test_portfolio


class TestMongo(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = connect('test', host="test-mongo")

        # get a fresh database
        cls.db.drop_database('test')


    def test_basic(self):
        # get a fresh database
        self.db.drop_database('test')

        # check database is indeed empty
        self.assertEqual(len([x for x in Price.objects]), 0)

        # create  new price series
        x = Price(security="IBM").save()
        self.assertIsNotNone(x.id)
        self.assertEqual(x.security, "IBM")

        with self.assertRaises(NotUniqueError):
            Price(security="IBM").save()

        pdt.assert_series_equal(x.series, pd.Series({}))
        self.assertIsNone(x.last)

        # set the series...
        x.series = pd.Series({1: 100.0, 2: 120.0, 3: 110.0})
        pdt.assert_series_equal(x.series, pd.Series({1: 100.0, 2: 120.0, 3: 110.0}))
        self.assertEqual(x.last, 3)

        a = pd.Series({2: 140, 3: 150})
        x.series = a
        pdt.assert_series_equal(x.series, pd.Series({1: 100.0, 2: 140.0, 3: 150.0}))
        pdt.assert_series_equal(a, pd.Series({2: 140, 3: 150}))

        z = Price.objects.upsert_one(security="HaHa")

        self.assertEqual(z.security, "HaHa")
        pdt.assert_series_equal(z.series, pd.Series({}))

        z.series = pd.Series({2: 145, 3: 150})

        pdt.assert_series_equal(z.series, pd.Series({2: 145, 3: 150}))

        pdt.assert_series_equal(x.series, pd.Series({1: 100.0, 2: 140.0, 3: 150.0}))


    def test_timeseries(self):
        self.db.drop_database('test')

        # check database is indeed empty
        self.assertEqual(len([x for x in Price.objects]), 0)

        # create  new price series
        x = Price(security="IBM").save()
        self.assertEqual(x.security, "IBM")

        x.series = test_portfolio().nav.apply(float)

        pdt.assert_series_equal(x.series, pd.Series({t: v for t, v in test_portfolio().nav.items()}))
        self.assertEqual(x.last, test_portfolio().index[-1])

    def test_speed(self):
        self.db.drop_database('test')

        x = Price(security="IBM").save()
        a = np.random.randn(1000000)
        x.series = pd.Series(data=a)
        x.save()

        #x.series
        #print(x.series)





