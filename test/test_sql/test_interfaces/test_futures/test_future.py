from unittest import TestCase

import pandas as pd

from pyutil.sql.interfaces.futures.category import FuturesCategory
from pyutil.sql.interfaces.futures.contract import Contract
from pyutil.sql.interfaces.futures.exchange import Exchange
from pyutil.sql.interfaces.futures.future import Future, Futures

from pyutil.sql.base import Base
from pyutil.sql.session import session_test


def future():
    # define an exchange
    e = Exchange(name="Chicago Mercantile Exchange", exch_code="CME")
    # define a category
    c = FuturesCategory(name="Equity Index")
    # define the future
    return Future(name="ES1 Index", internal="S&P 500 E-Mini Futures", quandl="CME/ES", exchange=e, category=c)


class TestFuture(TestCase):

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
        session = session_test(Base.metadata, echo=True)
        f = future()

        c1 = Contract(figi="BB1", notice=pd.Timestamp("2010-01-01").date())
        c2 = Contract(figi="BB2", notice=pd.Timestamp("2009-03-01").date())

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