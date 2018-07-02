import unittest

import pandas as pd
import pandas.util.testing as pdt

from pyutil.influx.client import Client
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


#def date2str(x):
#    return x.strftime("%Y-%m-%d")


class TestOwner(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client(host='test-influxdb', database="addepar")

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(dbname="addepar")


    def test_name(self):
        o = Owner(name=100, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
        self.assertEqual(o.currency, Currency(name="USD"))
        self.assertEqual(o.custodian, Custodian(name="UBS"))
        self.assertEqual(o.name, "100")

    def test_security(self):
        s = Security(name=120)
        self.assertEqual(str(s), "Security(120: None)")

    def test_currency(self):
        o = Owner(name="Peter")
        currency = Currency(name="CHF")
        o.currency = currency
        self.assertEqual(o.currency, currency)

    def test_custodian(self):
        custodian = Custodian(name="UBS Geneva")
        o = Owner(name="Peter", currency=Currency(name="USD"))
        o.custodian = custodian
        self.assertEqual(o.custodian, custodian)

    def test_return(self):
        o = Owner(name=100, currency=Currency(name="USD"))

        o.upsert_return(client=self.client, ts={})
        pdt.assert_series_equal(o.returns(self.client), pd.Series({}))

        o.upsert_return(client=self.client, ts={t1: 0.1, t2: 0.2})
        pdt.assert_series_equal(o.returns(self.client), pd.Series({t1.date(): 0.1, t2.date(): 0.2}, name="returns"))

    def test_volatility(self):
        o = Owner(name="100", currency=Currency(name="USD"))

        o.upsert_volatility(client=self.client, ts={})
        pdt.assert_series_equal(o.volatility(self.client), pd.Series({}))

        o.upsert_volatility(client=self.client, ts={t1: 0.1, t2: 0.3})
        pdt.assert_series_equal(o.volatility(self.client), pd.Series({t1.date(): 0.1, t2.date(): 0.3}, name="volatility"))

    def test_position(self):
        o = Owner(name="100", currency=Currency(name="USD"), custodian=Custodian(name="UBS"))

        # create a security
        s1 = Security(name="123")
        s1.reference[KIID] = 5

        # update a position in a security, you have to go through an owner! Position without an owner wouldn't make sense
        o.upsert_position(client=self.client, security=s1, custodian=o.custodian, ts={t1: 0.1, t2: 0.4})

        pdt.assert_frame_equal(o.position(client=self.client),
                               pd.DataFrame(columns=[t1.date(), t2.date()], index=["123"],  data=[[0.1, 0.4]]),
                               check_names=False)

        pdt.assert_frame_equal(o.position(client=self.client, sum=False, tail=1),
                               pd.DataFrame(columns=pd.Index([t2.date()]), index=["123"], data=[[0.4]]),
                               check_names=False)

        pdt.assert_frame_equal(o.position(client=self.client, sum=True, tail=1),
                               pd.DataFrame(columns=pd.Index([t2.date()]), index=["123", "Sum"], data=[[0.4], [0.4]]),
                               check_names=False)

        pdt.assert_frame_equal(o.position_by(client=self.client, tail=1),
                               pd.DataFrame(index=["123"], columns=[t2.date(), "KIID"], data=[[0.4, 5]]),
                               check_dtype=False, check_names=False)

        pdt.assert_frame_equal(o.position_by(client=self.client, index_col="KIID", tail=1),
                               pd.DataFrame(index=[5], columns=pd.Index([t2.date()]), data=[[0.4]]),
                               check_names=False)

        print(o.position_by(client=self.client, sum=True))

        pdt.assert_frame_equal(o.position_by(client=self.client, sum=True),
                               pd.DataFrame(index=["123", "Sum"], columns=[t1.date(), t2.date(), "KIID"],
                                            data=[[0.1, 0.4, 5], [0.1, 0.4, None]]), check_dtype=False,
                               check_names=False)

        pdt.assert_frame_equal(o.position_by(client=self.client, index_col="KIID", sum=True, tail=1),
                               pd.DataFrame(index=[5, "Sum"], columns=pd.Index([t2.date()]), data=[[0.4], [0.4]]),
                               check_names=False)

        self.assertTrue(o.position_by(client=self.client, index_col="MAFFAY").empty)


    def test_add_security(self):
        o = Owner(name="100", currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
        # create a security
        s1 = Security(name="123")
        s1.reference[KIID] = 5

        # note that the security is not linked to the owner yet
        self.assertTrue(o.reference_securities.empty)
        self.assertListEqual(o.securities, [])

        # update a position in a security, you have to go through an owner! Position without an owner wouldn't make sense
        o.upsert_position(client=self.client, security=s1, custodian=o.custodian, ts={t1: 0.1, t2: 0.4})

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
        s1.reference[NAME] = "Maffay"
        s1.reference[KIID] = 5

        # update the position in security s1
        o.upsert_position(client=self.client, security=s1, custodian=o.custodian, ts={t1: 0.1, t2: 0.4})

        pdt.assert_series_equal(o.kiid, pd.Series(index=["123"], data=[5]))
        pdt.assert_frame_equal(o.kiid_weighted(client=self.client, sum=False),
                               pd.DataFrame(index=["123"], columns=pd.Index([t1.date(), t2.date()]),
                                            data=[[0.5, 2.0]]), check_names=False)
        pdt.assert_frame_equal(o.kiid_weighted(client=self.client, sum=True),
                               pd.DataFrame(index=["123", "Sum"], columns=pd.Index([t1.date(), t2.date()]),
                                            data=[[0.5, 2.0], [0.5, 2.0]]), check_names=False)

        frame = pd.DataFrame(index=["Maffay"], columns=[t1.date(), t2.date()], data=[[0.5, 2.0]])
        pdt.assert_frame_equal(o.kiid_weighted_by(client=self.client, index_col="Name"), frame, check_names=False)

    def test_volatility(self):
        o = Owner(name=100, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))
        # c = Custodian(name="UBS")
        # create a security
        s1 = Security(name=123)
        s1.reference[KIID] = 5

        # update the position in security s1
        o.upsert_position(client=self.client, security=s1, custodian=o.custodian, ts={t1: 0.1, t2: 0.4})

        # update the volatility, note that you can update the volatility even after the security has been added to the owner
        s1.upsert_volatility(client=self.client, currency=o.currency.name, ts={t1: 2.5, t2: 2.5})

        #print(o.vola_securities(client=self.client))
        #print(o.securities)
        print(s1.volatility(client=self.client, currency=o.currency.name))

        pdt.assert_frame_equal(o.vola_securities(client=self.client),
                               pd.DataFrame(columns=pd.Index([t1.date(), t2.date()]), index=["123"],
                                            data=[[2.5, 2.5]]))

        pdt.assert_frame_equal(o.vola_weighted(client=self.client, sum=True),
                               pd.DataFrame(columns=pd.Index([t1.date(), t2.date()]), index=["123", "Sum"],
                                            data=[[0.25, 1.0],[0.25, 1.0]]), check_names=False)

        pdt.assert_frame_equal(o.vola_weighted_by(client=self.client),
                               pd.DataFrame(index=["123"], columns=[t1.date(), t2.date(), "KIID"],
                                            data=[[0.25, 1.0, 5]]), check_dtype=False, check_names=False)
        pdt.assert_frame_equal(o.vola_weighted_by(client=self.client, index_col="KIID"),
                               pd.DataFrame(index=[5], columns=pd.Index([t1.date(), t2.date()]),
                                            data=[[0.25, 1.0]]),
                               check_names=False)


