import pandas as pd
from unittest import TestCase

from pyutil.sql.base import Base
from pyutil.sql.db_risk import DatabaseRisk
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.custodian import Custodian
from pyutil.sql.interfaces.risk.owner import Owner
from pyutil.sql.interfaces.risk.security import Security, FIELDS
from pyutil.sql.model.ref import Field, DataType
from pyutil.sql.session import postgresql_db_test
from test.config import resource

import pandas.util.testing as pdt

t1 = pd.Timestamp("1978-11-16")
t2 = pd.Timestamp("1978-11-18")

KIID = FIELDS["Lobnek KIID"]
TICKER = FIELDS["Lobnek Ticker Symbol Bloomberg"]
NAME = FIELDS["name"]

class TestDatabaseRisk(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = postgresql_db_test(base=Base, echo=True, views=resource("addepar.ddl"))

        cls.cus1 = Custodian(name="UBS Geneva")
        cls.cur1 = Currency(name="USD")

        cls.s1 = Security(name=123)
        cls.s2 = Security(name=1000)

        cls.s1.reference[KIID] = 5
        cls.s1.price_upsert(ts={t1: 11.1, t2: 12.1})
        cls.s1.volatility_upsert(ts={t1: 11.1, t2: 12.1}, currency=cls.cur1)

        cls.session.add_all([cls.s1, cls.s2])

        # Don't forget to commit the data
        cls.session.commit()

        cls.o1 = Owner(name=100, currency=cls.cur1)
        cls.o1.reference[NAME] = "Peter Maffay"
        cls.o1.position_upsert(security=cls.s1, custodian=cls.cus1, ts={t1: 0.4, t2: 0.5})
        cls.o1.volatility_upsert(ts={t1: 0.3, t2: 0.3})

        cls.session.add(cls.o1)
        cls.session.commit()
        cls.db = DatabaseRisk(cls.session)


    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def test_owner(self):
        self.assertEqual(self.o1, self.db.owner(name="100"))

    def test_position(self):
        x = pd.DataFrame(index=[0],
                         columns=["owner", "security", "custodian", t1, t2],
                         data=[[100, 123, "UBS Geneva", 0.4, 0.5]])

        x.set_index(keys=["owner", "security", "custodian"], inplace=True)

        pdt.assert_series_equal(self.db.position.iloc[0], pd.Series({t1: 0.4, t2: 0.5}), check_names=False)


        #pdt.assert_frame_equal(x, self.db.position)


    #def test_fields(self):
    #    f = self.session.query(Field).filter_by(name="KIID").one()
    #    self.assertIsNotNone(f)
    #    self.assertEqual(f.name, "KIID")
    #    self.assertEqual(f.result, DataType.integer)

    def test_security(self):
        s = self.session.query(Security).filter_by(name="123").one()
        #self.assertEqual(s, self.s1)
        #self.assertIsNotNone(s)
        #self.assertEqual(s.get_reference("KIID"), 5)
        self.assertEqual(s.kiid, 5)
        self.assertIsNone(s.bloomberg_ticker)

    def test_reference_securities(self):
        pdt.assert_frame_equal(self.db.reference_securities, pd.DataFrame(index=[123], columns=["KIID"], data=[[5]]), check_names=False)

    def test_reference_owner(self):
        pdt.assert_frame_equal(self.db.reference_owner, pd.DataFrame(index=[100], columns=["Name"], data=[["Peter Maffay"]]), check_names=False)

    def test_reference_owner_securities(self):
        pdt.assert_frame_equal(self.db.reference_owner_securities.loc[100], self.db.reference_securities)

    def test_security_db(self):
        self.assertEqual(self.db.security(name="123"), self.session.query(Security).filter_by(name="123").one())
        self.assertEqual(self.db.security(name=123), self.session.query(Security).filter_by(name="123").one())

    def test_prices(self):
        pdt.assert_series_equal(self.db.prices.loc[123], pd.Series({t1: 11.1, t2: 12.1}), check_names=False)
        pdt.assert_series_equal(self.s1.price, pd.Series({t1: 11.1, t2: 12.1}))
        pdt.assert_series_equal(self.s1.price, self.db.prices.loc[123], check_names=False)

    def test_volatility_owner(self):
        pdt.assert_series_equal(self.db.volatility_owner.loc[100], pd.Series({t1: 0.3, t2: 0.3}), check_names=False)
        pdt.assert_series_equal(self.o1.volatility,pd.Series({t1: 0.3, t2: 0.3}), check_names=False)
        pdt.assert_series_equal(self.o1.volatility, self.db.volatility_owner.loc[100], check_names=False)

    def test_volatility_security(self):
        pdt.assert_series_equal(self.db.volatility_security.loc["USD"].loc[123], pd.Series({t1: 11.1, t2: 12.1}), check_names=False)

    def test_volatility_owner_securities(self):
        pdt.assert_series_equal(self.db.volatility_owner_securities.loc[100].loc[123],  pd.Series({t1: 11.1, t2: 12.1}), check_names=False)
