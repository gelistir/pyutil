from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.influx.client import Client
from pyutil.sql.base import Base
from pyutil.sql.db_risk import DatabaseRisk
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.custodian import Custodian
from pyutil.sql.interfaces.risk.owner import Owner
from pyutil.sql.interfaces.risk.security import Security, FIELDS
from pyutil.sql.session import postgresql_db_test
from test.config import resource

t1 = pd.Timestamp("1978-11-16")
t2 = pd.Timestamp("1978-11-18")

KIID = FIELDS["Lobnek KIID"]
TICKER = FIELDS["Lobnek Ticker Symbol Bloomberg"]
NAME = FIELDS["name"]

class TestDatabaseRisk(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = postgresql_db_test(base=Base, echo=True, views=resource("addepar.ddl"))
        cls.client = Client(host='test-influxdb', database="addepar")
        cls.cus1 = Custodian(name="UBS Geneva")
        cls.cur1 = Currency(name="USD")

        cls.s1 = Security(name=123)
        cls.s2 = Security(name=1000)

        cls.s1.reference[KIID] = 5

        cls.s1.upsert_price(client=cls.client, ts=pd.Series({t1: 11.1, t2: 12.1}))
        cls.s1.upsert_volatility(client=cls.client, ts=pd.Series({t1: 11.1, t2: 12.1}), currency=cls.cur1)

        cls.session.add_all([cls.s1, cls.s2])

        # Don't forget to commit the data
        cls.session.commit()

        cls.o1 = Owner(name=100, currency=cls.cur1)
        cls.o1.reference[NAME] = "Peter Maffay"
        # append the security
        cls.o1.securities.append(cls.s1)
        cls.o1.upsert_position(client=cls.client, security=cls.s1, custodian=cls.cus1, ts=pd.Series({t1: 0.4, t2: 0.5}))
        cls.o1.upsert_volatility(client=cls.client, ts=pd.Series({t1: 0.3, t2: 0.3}))
        cls.o1.upsert_return(client=cls.client, ts=pd.Series({t1: 0.2, t2: 0.1}))

        cls.session.add(cls.o1)
        cls.session.commit()
        cls.db = DatabaseRisk(client=cls.client, session=cls.session)


    @classmethod
    def tearDownClass(cls):
        cls.session.close()
        cls.client.drop_database("addepar")

    def test_owner(self):
        self.assertEqual(self.o1, self.db.owner(name="100"))

    def test_position(self):
        x = self.db.position.loc[("100","UBS Geneva","123")]
        pdt.assert_series_equal(x["weight"], pd.Series({t1: 0.4, t2: 0.5}), check_names=False)

    def test_returns(self):
        print(self.db.returns)
        pdt.assert_series_equal(self.db.returns["100"], pd.Series({t1: 0.2, t2: 0.1}), check_names=False)

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
        pdt.assert_series_equal(self.db.prices["123"], pd.Series({t1: 11.1, t2: 12.1}), check_names=False)

    def test_volatility_owner(self):
        pdt.assert_series_equal(self.db.volatility_owner["100"], pd.Series({t1: 0.3, t2: 0.3}), check_names=False)

    def test_volatility_security(self):
        print(self.db.volatility_security["123"])
        pdt.assert_series_equal(self.db.volatility_security["123"].loc["USD"], pd.Series({t1: 11.1, t2: 12.1}), check_names=False)

    # def test_volatility_owner_securities(self):
    #     print(self.db.volatility_owner)
    #
    #     pdt.assert_series_equal(self.db.volatility_owner_securities.loc[100].loc[123],  pd.Series({t1.date(): 11.1, t2.date(): 12.1}), check_names=False)
