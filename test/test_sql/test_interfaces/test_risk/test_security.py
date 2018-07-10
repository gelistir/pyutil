import unittest

import pandas as pd
import pandas.util.testing as pdt

from pyutil.influx.client import Client
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.security import Security

t0 = pd.Timestamp("1978-11-15")
t1 = pd.Timestamp("1978-11-16")


class TestSecurity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client(host='test-influxdb', database="addepar")

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(dbname="addepar")


    def test_name(self):
        s = Security(name=100, kiid=5, ticker="AAAAA US Equity")
        c = Currency(name="USD")
        self.assertEqual(s.name, "100")

        pdt.assert_series_equal(s.price(client=self.client), pd.Series({}))
        pdt.assert_series_equal(s.volatility(client=self.client, currency=c), pd.Series({}))

        self.assertEqual(s.discriminator, "Security")
        self.assertEqual(s.kiid, 5)

        self.assertEqual(s.bloomberg_ticker, "AAAAA US Equity")

    def test_price(self):
        s = Security(name=100)
        s.upsert_price(client=self.client, ts=pd.Series({t0: 11.0, t1: 12.1}))
        pdt.assert_series_equal(s.price(client=self.client), pd.Series(index=[t0, t1], data=[11.0, 12.1], name="price"))

        print(Security.prices_all(client=self.client))


    def test_volatility(self):
        s = Security(name=100)
        c = Currency(name="USD")
        s.upsert_volatility(client=self.client, currency=c, ts=pd.Series({t0: 11.0, t1: 12.1}))
        pdt.assert_series_equal(s.volatility(client=self.client, currency=c), pd.Series(index=[t0, t1], data=[11.0, 12.1], name="volatility"))
        print(Security.volatility_all(client=self.client))
