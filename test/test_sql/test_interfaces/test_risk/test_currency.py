import pandas as pd
import unittest

from pyutil.sql.model.ref import Field, DataType

import pandas.util.testing as pdt

from pyutil.sql.interfaces.risk.currency import Currency


class TestCurrency(unittest.TestCase):
    def test_currency(self):
        c1 = Currency(name="USD")
        c2 = Currency(name="CHF")

        self.assertEqual(c1.name, "USD")
        self.assertEqual(str(c1), "Currency(USD)")
        self.assertTrue(c2 < c1)

        # you can not change the name of a currency!
        with self.assertRaises(AttributeError):
            c1.name = "EUR"

        # access a reference field that has not been set
        f = Field(name="Maffay", result=DataType.string)
        self.assertIsNone(c2.get_reference(field=f))

        # Using the association proxy we can set fields
        c1.reference[f] = "Peter"
        self.assertEqual(c1.reference[f], "Peter")

        # update the time series linked to the currency
        c1.upsert_ts(name="HaHa", data={pd.Timestamp("2010-01-01"): 10.0})

        # there is no time series called Peter!
        with self.assertRaises(KeyError):
            c1.timeseries["Peter"]

        # here we don't have the KeyError!
        pdt.assert_series_equal(c1.get_timeseries(name="Peter"), pd.Series({}))

        pdt.assert_series_equal(c1.timeseries["HaHa"], pd.Series({pd.Timestamp("2010-01-01"): 10.0}))


