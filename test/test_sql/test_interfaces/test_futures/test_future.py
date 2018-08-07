from unittest import TestCase

import pandas as pd

from pyutil.sql.interfaces.futures.category import FuturesCategory
from pyutil.sql.interfaces.futures.contract import Contract
from pyutil.sql.interfaces.futures.exchange import Exchange
from pyutil.sql.interfaces.futures.future import Future

from pyutil.sql.base import Base
from pyutil.sql.session import session_test

import pandas.util.testing as pdt


def future():
    # define an exchange
    e = Exchange(name="Chicago Mercantile Exchange")
    # define a category
    c = FuturesCategory(name="Equity Index")
    # define the future
    return Future(name="ES1 Index", internal="S&P 500 E-Mini Futures", quandl="CME/ES", exchange=e, category=c)


class TestFuture(TestCase):
    @classmethod
    def setUpClass(cls):
        Future.client.recreate(dbname="test")

    def test_future(self):
        f = future()
        self.assertEqual(f.name, "ES1 Index")
        self.assertEqual(f.category.name, "Equity Index")
        self.assertEqual(f.exchange.name, "Chicago Mercantile Exchange")
        self.assertEqual(f.internal, "S&P 500 E-Mini Futures")
        self.assertEqual(f.quandl, "CME/ES")
        self.assertEqual(str(f), "Future(ES1 Index)")

    def test_future_no_contracts(self):
        # define an exchange
        f = future()
        self.assertIsNone(f.max_notice)
        self.assertListEqual(f.contracts, [])

    def test_future_with_contracts(self):
        session = session_test(Base.metadata, echo=False)
        f = future()

        c1 = Contract(figi="BB1", notice=pd.Timestamp("2010-01-01").date(), fut_month_yr="JAN 10")
        c2 = Contract(figi="BB2", notice=pd.Timestamp("2009-03-01").date(), fut_month_yr="MAR 09")

        f.contracts.append(c1)
        f.contracts.append(c2)
        session.add(f)
        session.commit()

        self.assertTrue(f.contracts[0].notice < f.contracts[1].notice)
        self.assertEqual(f.contracts[0].future, f)
        self.assertEqual(f.contracts[1].future, f)

        # You can not modify the underlying future of a contract!
        with self.assertRaises(AttributeError):
            c = f.contracts[0]
            c.future = f

        self.assertEqual(f.max_notice, pd.Timestamp("2010-01-01").date())
        self.assertListEqual(f.figis, ["BB2", "BB1"])
        self.assertEqual(f.contracts[0].quandl, "CME/ESH2009")
        self.assertTrue(f.contracts[0] < f.contracts[1])

    def test_rollmap(self):
        f = future()

        # add the contracts
        c1 = Contract(notice=pd.Timestamp("2014-01-01").date(), figi="A1",
                      bloomberg_symbol="AZ14 Comdty", fut_month_yr="Jan 14")
        c2 = Contract(notice=pd.Timestamp("2015-01-01").date(), figi="A2",
                      bloomberg_symbol="AZ15 Comdty", fut_month_yr="Jan 15")
        c3 = Contract(notice=pd.Timestamp("2016-01-01").date(), figi="A3",
                      bloomberg_symbol="AZ16 Comdty", fut_month_yr="Jan 16")
        c4 = Contract(notice=pd.Timestamp("2017-01-01").date(), figi="A4",
                      bloomberg_symbol="AZ17 Comdty", fut_month_yr="Jan 17")

        # use an abritrary order here...
        f.contracts.extend([c4, c2, c3, c1])
        t0 = pd.Timestamp("2014-12-11")

        x = f.roll_builder(offset_days=5).trunc(before=t0)

        pdt.assert_series_equal(x, pd.Series(index=pd.DatetimeIndex([t0.date(), pd.Timestamp("2014-12-27").date(), pd.Timestamp("2015-12-27").date()]), data=[c2,c3,c4]))


        x = f.roll_builder(offset_days=5).trunc(before=pd.Timestamp("2013-12-27"))

        pdt.assert_series_equal(x, pd.Series(index=pd.DatetimeIndex([pd.Timestamp("2013-12-27").date(), pd.Timestamp("2014-12-27").date(), pd.Timestamp("2015-12-27").date()]), data=[c2, c3, c4]))
