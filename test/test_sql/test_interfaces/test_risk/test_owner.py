import unittest

import pandas as pd
import pandas.util.testing as pdt

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
        ProductInterface.client.recreate(dbname="test")

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

        # x = pd.DataFrame(index=[t1, t2], columns=["110"], data=[[0.1], [0.2]])
        # y = pd.DataFrame({name: series for name, series in Owner.returns_all()})
        # pdt.assert_frame_equal(y, x, check_names=False)

    def test_volatility(self):
        # new owner!
        o = Owner(name="102", currency=Currency(name="USD"))

        o.upsert_volatility(ts=pd.Series({}))
        pdt.assert_series_equal(o.volatility, pd.Series({}))

        o.upsert_volatility(ts=pd.Series({t1: 0.1, t2: 0.3}))
        pdt.assert_series_equal(o.volatility, pd.Series({t1: 0.1, t2: 0.3}), check_names=False)

        # x = pd.DataFrame(index=[t1, t2], columns=["120"], data=[[0.1], [0.3]])
        # y = pd.DataFrame({name: series for name, series in Owner.volatility_all()})
        # pdt.assert_frame_equal(y, x, check_names=False)

    def test_position(self):
        o = Owner(name="103", currency=Currency(name="USD"), custodian=Custodian(name="UBS"))

        # create a security
        s1 = Security(name="123")
        s1.reference[KIID] = 5

        s2 = Security(name="211")
        s2.reference[KIID] = 7

        c1 = Custodian(name="UBS")
        c2 = Custodian(name="CS")

        o.securities.append(s1)
        o.securities.append(s2)

        # update a position in a security, you have to go through an owner! Position without an owner wouldn't make sense
        o.upsert_position(security=s1, custodian=c1, ts=pd.Series({t1: 0.1, t2: 0.4}))
        o.upsert_position(security=s2, custodian=c2, ts=pd.Series({t1: 0.5, t2: 0.5}))

        pdt.assert_series_equal(pd.Series({name: value for name, value in o.kiid}),
                                pd.Series(index=["123", "211"], data=[5, 7]))

        for position in o.position():
            print(position)

        for position in o.position(index_col="KIID"):
            print(position)

        frame = pd.DataFrame([position for position in o.position(index_col="KIID")]).set_index(keys=["security", "custodian", "date"])
        print(frame)

    def test_volatility_weighted(self):
        o = Owner(name=105, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
        # create a security
        s1 = Security(name=301)
        s1.reference[KIID] = 5

        # update the position in security s1
        o.securities.append(s1)
        o.upsert_position(security=s1, ts=pd.Series({t1: 0.1, t2: 0.4}))

        # update the volatility, note that you can update the volatility after the security has been added to the owner
        s1.upsert_volatility(currency=o.currency, ts=pd.Series({t1: 2.5, t2: 3.5}))

        for v in o.vola_weighted(index_col="KIID"):
            print(v)

        #assert False

        for v in o.vola():
            print(v)
        #print(list(o.vola()))
        for v in o.vola(index_col="KIID"):
            print(v)

        #assert False

        #x = pd.DataFrame(o.vola_securities()).transpose()
        #print(x)
        # assert False

        #ssert False

        # pdt.assert_frame_equal(x,
        #
        #                        pd.DataFrame(columns=pd.Index([t1, t2]), index=[s1],
        #                                     data=[[2.5, 3.5]]), check_names=False)
        #
        # # assert False
        #
        # pdt.assert_frame_equal(o.vola_weighted(),
        #                        pd.DataFrame(columns=pd.Index([t1, t2]), index=["123", "Sum"],
        #                                     data=[[0.25, 1.4], [0.25, 1.4]]), check_names=False)
        #
        # pdt.assert_frame_equal(o.vola_weighted_by(index_col="KIID"),
        #                        pd.DataFrame(index=[5], columns=[t1, t2],
        #                                     data=[[0.25, 1.4]]),
        #                        check_names=False)

    def test_reference_frame(self):
        o = Owner(name=100, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
        o.reference[NAME] = "Wurst"
        x = Owner.reference_frame(products=[o])
        pdt.assert_frame_equal(x, pd.DataFrame(index=["100"], columns=["Name"], data=[["Wurst"]]))

    def test_reference_securities_frame(self):
        o = Owner(name=100, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
        # create a security
        s1 = Security(name=410)
        s1.reference[KIID] = 5

        # update the position in security s1
        o.securities.append(s1)

        x = Owner.reference_frame(products=o.securities)

        pdt.assert_frame_equal(x, pd.DataFrame(index=["410"], columns=["KIID"], data=[[5]]))
