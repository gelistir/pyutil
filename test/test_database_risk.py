from unittest import TestCase

import pandas as pd
import numpy as np

from sqlalchemy.orm.exc import NoResultFound

from pyutil.sql.base import Base
from pyutil.sql.interfaces.risk.custodian import Custodian, Currency
from pyutil.sql.interfaces.risk.owner import Owner
from pyutil.sql.interfaces.risk.security import Security

from pyutil.data_risk import Database

import pandas.util.testing as pdt

from pyutil.sql.model.ref import DataType, Field, FieldType
from pyutil.sql.session import postgresql_db_test


class TestDatabase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session, connection_str = postgresql_db_test(base=Base)

        # Add some data into database
        c1 = Currency(name="USD")
        c2 = Currency(name="CHF")

        cus1 = Custodian(name="UBS")

        o1 = Owner(name="102", currency=c1)
        o2 = Owner(name="103", currency=c2)

        f1 = Field(name="XXX", result=DataType.integer)
        f2 = Field(name="Bloomberg Ticker", result=DataType.string, type=FieldType.other)

        o1.reference[f1] = "100"

        s1 = Security(name="123")
        s1.reference[f1] = "200"
        s1.reference[f2] = "HAHA US Equity"

        s2 = Security(name="456")

        c3 = Currency(name="EUR")

        cls.session.add_all([c1, c2, c3, cus1, o1, o2, f1, f2, s1, s2])
        cls.session.commit()

        #cls.client = test_client()
        cls.database = Database(session=cls.session)

    @classmethod
    def tearDownClass(cls):
        cls.database.close()

    def test_session(self):
        self.assertIsNotNone(self.database.session)

    def test_owners(self):
        self.assertEqual(self.database.owner(name="102"), Owner(name="102"))

    def test_securities(self):
        self.assertEqual(self.database.security(name="123"), Security(name="123"))
        self.assertEqual(self.database.security(name="123").bloomberg_ticker, "HAHA US Equity")

    def test_prices(self):
        # add prices to the database
        security = self.database.security(name="123")
        security.active = True

        ts = pd.Series({pd.Timestamp("2010-04-20"): 11.0, pd.Timestamp("2010-04-21"): 11.2})
        security.upsert_price(ts=ts)

        d = self.database.prices.sort_index(ascending=False)
        print(d)

        pdt.assert_series_equal(d[security.name], ts.sort_index(ascending=False), check_names=False)

    def test_returns(self):
        # compute returns
        owner = self.database.owner(name="102")

        ts = pd.Series({pd.Timestamp("2010-04-20"): 0.05, pd.Timestamp("2010-04-21"): 0.10})
        owner.upsert_returns(ts=ts)
        pdt.assert_series_equal(self.database.returns["102"], ts, check_names=False)

    def test_owner_volatility(self):
        # compute returns
        owner = self.database.owner(name="102")

        ts = pd.Series({pd.Timestamp("2010-04-20"): 0.05, pd.Timestamp("2010-04-21"): 0.10})
        owner.upsert_volatility(ts=ts)

        pdt.assert_series_equal(self.database.owner_volatility["102"], ts, check_names=False)

    def test_security_volatility(self):
        security = self.database.security(name="123")
        currency = self.database.currency(name="CHF")

        ts = pd.Series({pd.Timestamp("2010-04-20"): 0.05, pd.Timestamp("2010-04-21"): 0.10})
        security.upsert_volatility(currency=currency,ts=ts)

        #security.upsert_volatility(ts=ts, currency=currency)

        x = self.database.securities_volatility(currency=currency)
        pdt.assert_series_equal(x["123"], ts, check_names=False)

    def test_reference_owners(self):
        x = self.database.reference_owners
        f = pd.DataFrame(index=["102","103"], columns=["XXX"], data=[100, np.nan])
        f.index.name = "Entity ID"
        print(f)
        print(x)

        pdt.assert_frame_equal(f.reset_index(), x)

    def test_reference_securities(self):
        x = self.database.reference_securities.set_index("Entity ID").sort_index(axis=0)

        f = pd.DataFrame(index=["123", "456"], columns=["Bloomberg Ticker", "XXX"])
        f.index.name = "Entity ID"
        f["Bloomberg Ticker"]["123"] = "HAHA US Equity"
        f["XXX"]["123"] = 200
        pdt.assert_frame_equal(x.sort_index(axis=0), f)

    def test_custodian(self):
        self.assertEqual(self.database.custodian(name="UBS"), Custodian(name="UBS"))
        with self.assertRaises(NoResultFound):
            self.database.custodian(name="NONONO")
