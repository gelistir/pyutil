import unittest
import pandas as pd

from pyutil.sql.interfaces.risk.security import Security, FIELDS as FIELDSSECURITY
from pyutil.sql.interfaces.risk.owner import Owner, FIELDS as FIELDSOWNER
import pandas.util.testing as pdt

from pyutil.sql.interfaces.risk.currency import Currency

t0 = pd.Timestamp("1978-11-15")
t1 = pd.Timestamp("1978-11-16")
t2 = pd.Timestamp("1978-11-18")

KIID = FIELDSSECURITY["Lobnek KIID"]
CUSTODIAN = FIELDSOWNER["15. Custodian Name"]
NAME = FIELDSOWNER["name"]


class TestOwner(unittest.TestCase):
    def test_name(self):
        usd = Currency(name="USD")
        o = Owner(name=100, currency=usd)

        self.assertEqual(o.currency, usd)
        self.assertEqual(str(o), "Owner(100: None)")
        self.assertIsNone(o.current_position)

        with self.assertRaises(AttributeError):
            chf = Currency(name="CHF")
            o.currency = chf

    def test_return(self):
        usd = Currency(name="USD")
        o = Owner(name=100, currency=usd)
        o.returns_upsert(ts={t1: 0.1, t2: 0.2})
        pdt.assert_series_equal(o.returns, pd.Series({t1: 0.1, t2: 0.2}))
        pdt.assert_series_equal(o.nav, pd.Series(index=[t0, t1, t2], data=[1.0, 1.1, 1.32]))

    def test_volatility(self):
        o = Owner(name=100, currency=Currency(name="USD"))
        o.volatility_upsert(ts={t1: 0.1, t2: 0.3})
        pdt.assert_series_equal(o.volatility, pd.Series({t1: 0.1, t2: 0.3}))

    def test_add_security(self):
        # introduce a currency
        usd = Currency(name="USD")
        self.assertEqual(usd.name, "USD")

        # create an owner
        o = Owner(name=100, currency=usd)
        self.assertEqual(o.name, "100")
        self.assertEqual(o.currency, usd)

        # create a security
        s1 = Security(name=123)
        s1.reference[KIID] = "5"
        s1.reference[CUSTODIAN] = "UBS"
        # self.assertEqual(s1.entity_id, 123)
        self.assertEqual(s1.kiid, 5)
        self.assertEqual(s1.reference[CUSTODIAN], "UBS")

        # note that the security is not linked to the owner yet
        self.assertTrue(o.reference_securities.empty)
        # appending securities can be done explicitly as demonstrated here or by defining a position in the security
        o.securities.append(s1)
        pdt.assert_frame_equal(o.reference_securities,
                               pd.DataFrame(index=["123"], columns=["Custodian", "KIID"], data=[["UBS", 5]]),
                               check_dtype=False)
        # let's remove the security we have just added!
        o.securities.pop(0)

        # update the prices for the security
        s1.price_upsert(ts={t1: 11.0, t2: 12.0})
        pdt.assert_series_equal(s1.price, pd.Series({t1: 11.0, t2: 12.0}))

        # update a position in a security, you have to go through an owner! Position without an owner wouldn't make sense
        self.assertListEqual(o.securities, [])
        o.position_upsert(security=s1, ts={t1: 0.1, t2: 0.4})
        self.assertListEqual(o.securities, [s1])

        pdt.assert_frame_equal(o.position,
                               pd.DataFrame(columns=[t1, t2], index=["123"], data=[[0.1, 0.4]]))

        pdt.assert_series_equal(o.current_position, pd.Series({"123": 0.4}))

        pdt.assert_frame_equal(o.position_by(), pd.DataFrame(index=["123"], columns=[t1, t2, "Custodian", "KIID"],
                                                             data=[[0.1, 0.4, "UBS", 5]]), check_dtype=False)

        pdt.assert_frame_equal(o.position_by(index="KIID"),
                               pd.DataFrame(index=[5], columns=pd.Index([t1, t2]), data=[[0.1, 0.4]]),
                               check_names=False)

        # update the volatility, note that you can update the volatility even after the security has been added to the owner
        s1.volatility_upsert(currency=usd, ts={t1: 2.5, t2: 2.5})
        pdt.assert_frame_equal(o.vola_securities, pd.DataFrame(columns=[t1, t2], index=["123"], data=[[2.5, 2.5]]))

        pdt.assert_frame_equal(o.vola_securities, pd.DataFrame(columns=[t1, t2], index=["123"], data=[[2.5, 2.5]]))

        pdt.assert_frame_equal(o.vola_weighted, pd.DataFrame(columns=[t1, t2], index=["123"], data=[[0.25, 1.0]]))


        pdt.assert_frame_equal(o.vola_weighted_by(), pd.DataFrame(index=["123"], columns=[t1, t2, "Custodian", "KIID"],
                                                                  data=[[0.25, 1.0, "UBS", 5]]), check_dtype=False)
        pdt.assert_frame_equal(o.vola_weighted_by(index="KIID"),
                               pd.DataFrame(index=[5], columns=pd.Index([t1, t2]), data=[[0.25, 1.0]]),
                               check_names=False)

        self.assertListEqual(o.securities, [s1])
        pdt.assert_series_equal(o.kiid, pd.Series(index=["123"], data=[5]))
        pdt.assert_frame_equal(o.kiid_weighted, pd.DataFrame(index=["123"], columns=[t1, t2], data=[[0.5, 2.0]]))
