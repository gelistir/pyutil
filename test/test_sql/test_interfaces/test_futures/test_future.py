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
        session.add(f)
        session.commit()

        c1 = Contract(figi="BB1", notice=pd.Timestamp("2010-01-01").date())
        c2 = Contract(figi="BB2", notice=pd.Timestamp("2009-03-01").date())

        f.contracts.append(c1)
        f.contracts.append(c2)
        session.commit()
        #print(sorted(f.contracts)) #, key=lambda x: x.notice))
        print(f.contracts[0] < f.contracts[1])
        print(f.contracts[0])
        print(f.contracts[1])

        print(f.contracts)

class TestFutures(TestCase):
    def test_futures(self):
        # create a future
        f = future()
        # make a "dictionary" of futures
        x = Futures([f])
        self.assertEqual(x["ES1 Index"], f)
