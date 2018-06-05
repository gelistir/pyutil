import os
import unittest
from collections import OrderedDict

import pandas as pd


import pandas.util.testing as pdt
import sqlalchemy

from pyutil.sql.base import Base
from pyutil.sql.db_risk import Database
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.security import Security
from pyutil.sql.interfaces.risk.security import FIELDS as FIELDSSECURITY
from pyutil.sql.session import test_postgresql_db
from test.config import resource

t1 = pd.Timestamp("1978-11-16")
t2 = pd.Timestamp("1978-11-18")

KIID = FIELDSSECURITY["Lobnek KIID"]
TICKER = FIELDSSECURITY["Lobnek Ticker Symbol Bloomberg"]


class TestSecurity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = test_postgresql_db()

        Base.metadata.create_all(cls.session.bind)

        # add views to database
        file = resource("addepar.ddl")

        with open(file) as file:
            cls.session.bind.execute(file.read())

        c1 = Currency(name="USD")
        c2 = Currency(name="CHF")

        # add currencies to Database
        cls.session.add_all([c1, c2])

        s1 = Security(name=123)
        s2 = Security(name=1000)

        s1.reference[KIID] = 5
        s1.price_upsert(ts={t1: 11.1, t2: 12.1})
        s1.volatility_upsert(ts={t1: 11.1, t2: 12.1}, currency=c1)

        cls.session.add_all([s1, s2])

        # Don't forget to commit the data
        cls.session.commit()

        cls.db = Database(cls.session)

    def test_sec1(self):
        s1 = self.session.query(Security).filter_by(name="123").one()
        # no currency has been defined
        self.assertEqual(str(s1), "Security(123: None)")

        # get the price
        pdt.assert_series_equal(s1.price, pd.Series({t1: 11.1, t2: 12.1}))

    def test_cur1(self):
        c1 = self.session.query(Currency).filter_by(name="USD").one()
        self.assertEqual(str(c1), "Currency(USD)")


    def test_vola1(self):
        s = self.session.query(Security).filter_by(name="123").one()
        c = self.session.query(Currency).filter_by(name="USD").one()

        #s.volatility_upsert(ts={t1: 11.1, t2: 12.1}, currency=c)
        pdt.assert_frame_equal(s.volatility, pd.DataFrame(index=[t1, t2], columns=[c], data=[[11.1],[12.1]]))

        #print(self.db.volatility_security)
        #assert False

    def test_ref1(self):
        #ses = sess()
        db = Database(session=self.session)

        s = self.session.query(Security).filter_by(name="123").one()
        self.assertEqual(s.reference[KIID], 5)
        self.assertEqual(s.kiid, 5)
        print(db.reference_securities)

    def test_ticker(self):
        # test the ticker!
        s = self.session.query(Security).filter_by(name="123").one()
        self.assertIsNone(s.bloomberg_ticker)

        s.reference[TICKER] = "HAHA US Equity"
        self.assertEqual(s.bloomberg_ticker, "HAHA US Equity")

    def test_to_dict(self):
        s = self.session.query(Security).filter_by(name="123").one()
        x = s.to_html_dict()
        print(x)
        assert "nav" in x
        assert "drawdown" in x
        assert "volatility" in x


    #def test_securities(self):
    #    s1 = Security(name=100)
    #    s2 = Security(name=1300)

    #    o = Securities([s1,s2])
    #    self.assertEqual(str(o), "       100   Security(100: None)\n      1300   Security(1300: None)")
    #    self.assertDictEqual(o.to_html_dict(), {'columns': ['Entity ID'], 'data': [OrderedDict([('Entity ID', '100')]), OrderedDict([('Entity ID', '1300')])]})

    def test_price_securties(self):
        pdt.assert_frame_equal(self.db.prices, pd.DataFrame(index=[123], columns=[t1, t2], data=[[11.1, 12.1]]), check_names=False)


    # def test_securities_volatility(self):
    #     s1 = Security(name=100)
    #     s2 = Security(name=1300)
    #
    #     c1 = Currency(name="USD")
    #     c2 = Currency(name="CHF")
    #
    #     s1.volatility_upsert(ts={pd.Timestamp("2010-01-01"): 50.0}, currency=c1)
    #     s1.volatility_upsert(ts={pd.Timestamp("2010-01-01"): 40.0}, currency=c2)
    #
    #     o = Securities([s1,s2])
    #
    #     index = pd.MultiIndex(levels=[['100'], [pd.Timestamp("2010-01-01")], ['CHF', 'USD']],
    #                           labels = [[0, 0], [0, 0], [0, 1]],
    #                           names = ['Security', 'Date', 'Currency'])
    #
    #     #pdt.assert_frame_equal(o.volatilities, pd.DataFrame(index=index, columns=["Volatility"], data=[[40.0], [50.0]]))

