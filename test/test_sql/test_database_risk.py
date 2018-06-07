import pandas as pd
from unittest import TestCase

from pyutil.sql.base import Base
from pyutil.sql.db_risk import Database
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.custodian import Custodian
from pyutil.sql.interfaces.risk.owner import Owner
from pyutil.sql.interfaces.risk.security import Security, FIELDS
from pyutil.sql.model.ref import Field, DataType
from pyutil.sql.session import test_postgresql_db
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
        cls.session = test_postgresql_db(echo=True)

        Base.metadata.create_all(cls.session.bind)

        # add views to database
        file = resource("addepar.ddl")

        with open(file) as file:
            cls.session.bind.execute(file.read())

        cls.cus1 = Custodian(name="UBS Geneva")
        cls.cur1 = Currency(name="USD")

        s1 = Security(name=123)
        s2 = Security(name=1000)

        s1.reference[KIID] = 5
        s1.price_upsert(ts={t1: 11.1, t2: 12.1})
        s1.volatility_upsert(ts={t1: 11.1, t2: 12.1}, currency=cls.cur1)

        cls.session.add_all([s1, s2])

        # Don't forget to commit the data
        cls.session.commit()

        #cls.db = Database(cls.session)



        cls.o1 = Owner(name=100, currency=cls.cur1)
        cls.o1.reference[NAME] = "Peter Maffay"
        cls.o1.position_upsert(security=s1, custodian=cls.cus1, ts={t1: 0.4, t2: 0.5})
        cls.o1.volatility_upsert(ts={t1: 0.3, t2: 0.3})

        cls.session.commit()
        cls.db = Database(cls.session)


    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def test_fields(self):
        f = self.session.query(Field).filter_by(name="KIID").one()
        self.assertIsNotNone(f)
        self.assertEqual(f.name, "KIID")
        self.assertEqual(f.result, DataType.integer)

    def test_security(self):
        s = self.session.query(Security).filter_by(name="123").one()
        self.assertIsNotNone(s)
        self.assertEqual(s.get_reference("KIID"), 5)

    def test_reference_securities(self):
        pdt.assert_frame_equal(self.db.reference_securities, pd.DataFrame(index=[123], columns=["KIID"], data=[[5]]), check_names=False)

    def test_reference_owner(self):
        pdt.assert_frame_equal(self.db.reference_owner, pd.DataFrame(index=[100], columns=["Name"], data=[["Peter Maffay"]]), check_names=False)

    def test_reference_owner_securities(self):
        pdt.assert_frame_equal(self.db.reference_owner_securities.loc[100], self.db.reference_securities)

    def test_security(self):
        self.assertEqual(self.db.security(name="123"), self.session.query(Security).filter_by(name="123").one())
        self.assertEqual(self.db.security(name=123), self.session.query(Security).filter_by(name="123").one())

    def test_prices(self):
        pdt.assert_series_equal(self.db.prices.loc[123], pd.Series({t1: 11.1, t2: 12.1}), check_names=False)

    def test_volatility_owner(self):
        pdt.assert_series_equal(self.db.volatility_owner.loc[100], pd.Series({t1: 0.3, t2: 0.3}), check_names=False)

    def test_volatility_security(self):
        pdt.assert_series_equal(self.db.volatility_security.loc["USD"].loc[123], pd.Series({t1: 11.1, t2: 12.1}), check_names=False)

    def test_volatility_owner_securities(self):
        pdt.assert_series_equal(self.db.volatility_owner_securities.loc[100].loc[123],  pd.Series({t1: 11.1, t2: 12.1}), check_names=False)
