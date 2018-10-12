import unittest

import pandas as pd
import pandas.util.testing as pdt

from pyutil.influx.client import test_client
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.custodian import Custodian
from pyutil.sql.interfaces.risk.owner import Owner, FIELDS as FIELDSOWNER
from pyutil.sql.interfaces.risk.security import Security, FIELDS as FIELDSSECURITY

t0 = pd.Timestamp("1978-11-15")
t1 = pd.Timestamp("1978-11-16")
t2 = pd.Timestamp("1978-11-18")

KIID = FIELDSSECURITY["Lobnek KIID"]
CUSTODIAN = FIELDSOWNER["15. Custodian Name"]
NAME = FIELDSOWNER["name"]


class TestOwner(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        ProductInterface.client = test_client() #.recreate(dbname="test")

        # create an owner
        cls.o1 = Owner(name=100, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))

    @classmethod
    def tearDownClass(cls):
        ProductInterface.client.close()

    def test_name(self):
        o = Owner(name=100, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
        self.assertEqual(o.currency, Currency(name="USD"))
        self.assertEqual(o.custodian, Custodian(name="UBS"))
        self.assertEqual(o.name, "100")
        self.assertEqual(str(o), "Owner(100: None)")

    def test_return(self):
        # new owner!
        o = Owner(name=101, currency=Currency(name="USD"))

        # update with an empty return series
        o.upsert_return(ts=pd.Series({}))
        pdt.assert_series_equal(o.returns, pd.Series({}))
        pdt.assert_series_equal(o.nav, pd.Series({}))
        pdt.assert_series_equal(o.volatility, pd.Series({}))

        # now update with a proper series
        o.upsert_return(ts=pd.Series({t1: 0.1, t2: 0.2}))
        pdt.assert_series_equal(o.returns, pd.Series({t1: 0.1, t2: 0.2}), check_names=False)
        pdt.assert_series_equal(o.nav, pd.Series({t0: 1.0, t1: 1.1, t2: 1.1 * 1.2}), check_names=False)

    def test_volatility(self):
        # new owner!
        o = Owner(name="102", currency=Currency(name="USD"))

        o.upsert_volatility(ts=pd.Series({}))
        pdt.assert_series_equal(o.volatility, pd.Series({}))

        o.upsert_volatility(ts=pd.Series({t1: 0.1, t2: 0.3}))
        pdt.assert_series_equal(o.volatility, pd.Series({t1: 0.1, t2: 0.3}), check_names=False)

    def test_position(self):
        o = Owner(name="103", currency=Currency(name="USD"), custodian=Custodian(name="UBS"))

        # create a security
        s1 = Security(name="123")
        s1.reference[KIID] = 5

        s2 = Security(name="211")
        s2.reference[KIID] = 7

        c1 = Custodian(name="UBS")
        c2 = Custodian(name="CS")

        # update a position in a security, you have to go through an owner! Position without an owner wouldn't make sense
        o.upsert_position(security=s1, custodian=c1, ts=pd.Series({t1: 0.1, t2: 0.4}))
        o.upsert_position(security=s2, custodian=c2, ts=pd.Series({t1: 0.5, t2: 0.5}))

        print(o.position())

        pdt.assert_series_equal(pd.Series({security.name: value for security, value in o.kiid.items()}).sort_index(), pd.Series(index=["123", "211"], data=[5, 7]))

        #for position in o.position():
        #    print(position)

        #assert False

        print(o.position("KIID"))
        for position in o.position(index_col="KIID"):
            print(position)

        #frame = pd.DataFrame([position for position in o.position(index_col="KIID")]).set_index(keys=["security", "custodian", "date"])
        #print(frame)

    def test_volatility_weighted(self):
        o = Owner(name=105, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
        # create a security
        s1 = Security(name=301)
        s1.reference[KIID] = 5

        # update the position in security s1
        o.upsert_position(security=s1, ts=pd.Series({t1: 0.1, t2: 0.4}))

        # update the volatility, note that you can update the volatility after the security has been added to the owner
        s1.upsert_volatility(currency=o.currency, ts=pd.Series({t1: 2.5, t2: 3.5}))

        print(o.vola_weighted())

        print(o.vola_weighted(index_col="KIID"))

        #    print(v)

        print(o.vola())

        #    print(v)

        print(o.vola(index_col="KIID"))

        #assert False


    def test_reference_frame(self):
        o = Owner(name=100, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
        o.reference[NAME] = "Wurst"
        x = Owner.reference_frame(products=[o])
        pdt.assert_frame_equal(x, pd.DataFrame(index=["100"], columns=["Name"], data=[["Wurst"]]))

    def test_reference_securities_frame(self):
        # create a security
        s1 = Security(name=410)
        s1.reference[KIID] = 5

        x = Owner.reference_frame(products=[s1])

        pdt.assert_frame_equal(x, pd.DataFrame(index=["410"], columns=["KIID"], data=[[5]]))

    # def test_positions(self):
    #     o1 = Owner(name=152, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
    #     o2 = Owner(name=153, currency=Currency(name="EUR"), custodian=Custodian(name="CS"))
    #
    #     s1 = Security(name=460)
    #     s2 = Security(name=461)
    #
    #     o1.upsert_position(security=s1, ts=pd.Series({t1: 0.1, t2: 0.4}))
    #     o2.upsert_position(security=s1, ts=pd.Series({t2: 0.4}))
    #     o1.upsert_position(security=s2, ts=pd.Series({t1: 0.7}))
    #
    #     p = set(Owner.positions(owners=[o1, o2], index_col=None))
    #     self.assertEqual(len(p), 4)
    #
    #     #assert False
    #
    #     # todo: finish the test
    #
    # def test_volatilities(self):
    #     o1 = Owner(name=152, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
    #     o2 = Owner(name=153, currency=Currency(name="EUR"), custodian=Custodian(name="CS"))
    #
    #     o1.upsert_volatility(ts=pd.Series({t1: 0.1, t2: 0.4}))
    #     o2.upsert_volatility(ts=pd.Series({t2: 0.4}))
    #
    #     for owner, volatility in Owner.volatilities(owners=[o1, o2]):
    #         print(owner)
    #         print(volatility)
    #
    #     # todo: finish the test

    def test_double_position(self):
        o = Owner(name=999, currency=Currency(name="USD"), custodian=Custodian(name="CS"))
        s = Security(name=777)
        x = pd.Series({t1.date(): 0.1})
        o.upsert_position(s, ts=x)
        a = o.position()

        o.upsert_position(s, ts=x)
        b = o.position()

        pdt.assert_series_equal(a,b)
