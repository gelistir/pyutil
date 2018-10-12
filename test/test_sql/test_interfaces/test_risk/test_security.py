import unittest

import pandas as pd
import pandas.util.testing as pdt

from pyutil.influx.client import test_client
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.security import Security

t0 = pd.Timestamp("1978-11-15")
t1 = pd.Timestamp("1978-11-16")


class TestSecurity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        client = test_client()
        client.recreate(dbname="test")
        ProductInterface.client = client


    @classmethod
    def tearDownClass(cls):
        ProductInterface.client.close()

    def test_name(self):
        s = Security(name=100, kiid=5, ticker="AAAAA US Equity")
        c = Currency(name="USD")
        self.assertEqual(s.name, "100")

        self.assertListEqual(list(s.price), [])
        self.assertListEqual(list(s.volatility(currency=c)), [])

        self.assertEqual(s.discriminator, "Security")
        self.assertEqual(s.kiid, 5)

        self.assertEqual(s.bloomberg_ticker, "AAAAA US Equity")
        self.assertEqual(str(s), "Security(100: None)")
        self.assertEqual(s.bloomberg_scaling, 1.0)


    def test_price(self):
        s = Security(name=100)
        s.upsert_price(ts=pd.Series({t0: 11.0, t1: 12.1}))
        pdt.assert_series_equal(s.price, pd.Series(index=[t0, t1], data=[11.0, 12.1]), check_names=False)

    def test_volatility(self):
        s = Security(name=100)
        c = Currency(name="USD")
        s.upsert_volatility(currency=c, ts=pd.Series({t0: 11.0, t1: 12.1}))

        #x = pd.Series({data.date: data.value for data in s.volatility(currency=c)})
        pdt.assert_series_equal(s.volatility(currency=c), pd.Series(index=[t0, t1], data=[11.0, 12.1]), check_names=False)

    def test_reference_frame(self):
        s1 = Security(name=100, kiid=4)
        s2 = Security(name=110, kiid=3)
        s3 = Security(name=120, kiid=5)

        x = pd.DataFrame(index=[s1, s2, s3], columns=["KIID"], data=[[4],[3],[5]])
        pdt.assert_frame_equal(x, Security.reference_frame(products=sorted([s1, s2, s3])))

