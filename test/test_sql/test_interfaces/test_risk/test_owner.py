import unittest

import pandas as pd
import pandas.util.testing as pdt

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
        Owner.client.recreate(dbname="test")

        # create an owner
        cls.o = Owner(name=100, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))

    def test_name(self):
        self.assertEqual(self.o.currency, Currency(name="USD"))
        self.assertEqual(self.o.custodian, Custodian(name="UBS"))
        self.assertEqual(self.o.name, "100")
        self.assertEqual(str(self.o), "Owner(100: None)")

    def test_return(self):
        # new owner!
        o = Owner(name=110, currency=Currency(name="USD"))

        o.upsert_return(ts=pd.Series({}))
        pdt.assert_series_equal(o.returns, pd.Series({}))
        pdt.assert_series_equal(o.nav, pd.Series({}))

        o.upsert_return(ts=pd.Series({t1: 0.1, t2: 0.2}))
        pdt.assert_series_equal(o.returns, pd.Series({t1: 0.1, t2: 0.2}, name="return"))
        pdt.assert_series_equal(o.nav, pd.Series({t0: 1.0, t1: 1.1, t2: 1.1*1.2}, name="nav"))

        x = pd.DataFrame(index=[t1,t2], columns=["110"], data=[[0.1],[0.2]])
        pdt.assert_frame_equal(Owner.returns_all(), x, check_names=False)

    def test_volatility(self):
        # new owner!
        o = Owner(name="120", currency=Currency(name="USD"))

        o.upsert_volatility(ts=pd.Series({}))
        pdt.assert_series_equal(o.volatility, pd.Series({}))

        o.upsert_volatility(ts=pd.Series({t1: 0.1, t2: 0.3}))
        pdt.assert_series_equal(o.volatility, pd.Series({t1: 0.1, t2: 0.3}, name="volatility"))

        x = pd.DataFrame(index=[t1, t2], columns=["120"], data=[[0.1],[0.3]])
        pdt.assert_frame_equal(Owner.volatility_all(), x, check_names=False)

    def test_position(self):
        o = Owner(name="130", currency=Currency(name="USD"), custodian=Custodian(name="UBS"))

        # create a security
        s1 = Security(name="123")
        s1.reference[KIID] = 5

        c1 = Custodian(name="UBS")

        o.securities.append(s1)
        # update a position in a security, you have to go through an owner! Position without an owner wouldn't make sense
        o.upsert_position(security=s1, custodian=c1, ts=pd.Series({t1: 0.1, t2: 0.4}))


        pdt.assert_frame_equal(o.position(),
                               pd.DataFrame(columns=[t1, t2], index=["123"],  data=[[0.1, 0.4]]),
                               check_names=False)

        pdt.assert_frame_equal(o.position(sum=False, tail=1),
                               pd.DataFrame(columns=pd.Index([t2]), index=["123"], data=[[0.4]]),
                               check_names=False)

        pdt.assert_frame_equal(o.position(sum=True, tail=1),
                               pd.DataFrame(columns=pd.Index([t2]), index=["123", "Sum"], data=[[0.4], [0.4]]),
                               check_names=False)

        pdt.assert_frame_equal(o.position_by(index_col="KIID", tail=1),
                               pd.DataFrame(index=[5], columns=[t2], data=[[0.4]]),
                               check_names=False)

        pdt.assert_frame_equal(o.position_by(index_col="KIID", sum=True, tail=1),
                               pd.DataFrame(index=[5, "Sum"], columns=pd.Index([t2]), data=[[0.4], [0.4]]),
                               check_names=False)

        self.assertTrue(o.position_by(index_col="MAFFAY").empty)

        print(Owner.position_all())

        x = Owner.position_all().loc["130"].loc["123"].loc["UBS"]
        y = pd.Series(index=[t1, t2], data=[0.1, 0.4])
        pdt.assert_series_equal(x, y, check_names=False)


    def test_add_security(self):
        o = Owner(name="100", currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
        # create a security
        s1 = Security(name="123")
        s1.reference[KIID] = 5

        # note that the security is not linked to the owner yet
        self.assertTrue(o.reference_securities.empty)
        self.assertListEqual(o.securities, [])

        o.securities.append(s1)

        self.assertListEqual(o.securities, [s1])

        pdt.assert_frame_equal(o.reference_securities,
                               pd.DataFrame(index=["123"], columns=["KIID"], data=[[5]]), check_dtype=False)
        self.assertListEqual(o.securities, [s1])

        # let's remove the security we have just added!
        o.securities.pop(0)
        self.assertTrue(o.reference_securities.empty)

    def test_kiid(self):
        o = Owner(name='100', currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
        o.reference[NAME] = "Peter"

        # create a security
        s1 = Security(name="123")
        c1 = Custodian(name="UBS")

        s1.reference[NAME] = "Maffay"
        s1.reference[KIID] = 5

        o.securities.append(s1)
        # update the position in security s1
        o.upsert_position(security=s1, custodian=c1, ts=pd.Series({t1: 0.1, t2: 0.4}))

        pdt.assert_series_equal(o.kiid, pd.Series(index=["123"], data=[5]))
        pdt.assert_frame_equal(o.kiid_weighted(sum=False),
                               pd.DataFrame(index=["123"], columns=pd.Index([t1, t2]),
                                            data=[[0.5, 2.0]]), check_names=False)

        pdt.assert_frame_equal(o.kiid_weighted(sum=True),
                               pd.DataFrame(index=["123", "Sum"], columns=pd.Index([t1, t2]),
                                            data=[[0.5, 2.0], [0.5, 2.0]]), check_names=False)

        frame = pd.DataFrame(index=["Maffay"], columns=[t1, t2], data=[[0.5, 2.0]])
        pdt.assert_frame_equal(o.kiid_weighted_by(index_col="Name"), frame, check_names=False)

    def test_volatility_weighted(self):
        o = Owner(name=100, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
        # create a security
        s1 = Security(name=123)
        s1.reference[KIID] = 5

        c1 = Custodian(name="UBS")

        # update the position in security s1
        o.securities.append(s1)
        o.upsert_position( security=s1, custodian=c1, ts=pd.Series({t1: 0.1, t2: 0.4}))

        # update the volatility, note that you can update the volatility even after the security has been added to the owner
        s1.upsert_volatility(currency=o.currency, ts=pd.Series({t1: 2.5, t2: 3.5}))

        pdt.assert_frame_equal(o.vola_securities(),
                               pd.DataFrame(columns=pd.Index([t1, t2]), index=["123"],
                                            data=[[2.5, 3.5]]))

        pdt.assert_frame_equal(o.vola_weighted(sum=True),
                               pd.DataFrame(columns=pd.Index([t1, t2]), index=["123", "Sum"],
                                            data=[[0.25, 1.4],[0.25, 1.4]]), check_names=False)

        pdt.assert_frame_equal(o.vola_weighted_by(index_col="KIID"),
                               pd.DataFrame(index=[5], columns=[t1, t2],
                                            data=[[0.25, 1.4]]),
                               check_names=False)


    def test_reference_frame(self):
        o = Owner(name=100, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
        o.reference[NAME] = "Wurst"
        x = Owner.reference_frame(products=[o])
        pdt.assert_frame_equal(x, pd.DataFrame(index=["100"], columns=["Name"], data=[["Wurst"]]))

    def test_reference_securities_frame(self):
        o = Owner(name=100, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
        # create a security
        s1 = Security(name=123)
        s1.reference[KIID] = 5

        # update the position in security s1
        o.securities.append(s1)

        x = Owner.reference_frame(products=o.securities)

        pdt.assert_frame_equal(x, pd.DataFrame(index=["123"], columns=["KIID"], data=[[5]]))

