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
        # create an owner
        cls.o1 = Owner(name=100, currency=Currency(name="USD"), custodian=Custodian(name="UBS"))

    def test_name(self):
        self.assertEqual(self.o1.currency, Currency(name="USD"))
        self.assertEqual(self.o1.custodian, Custodian(name="UBS"))
        self.assertEqual(self.o1.name, "100")
        self.assertEqual(str(self.o1), "Owner(100: None)")

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

        s1.upsert_volatility(currency=Currency(name="USD"), ts=pd.Series({t1: 5, t2: 6.0}))
        s2.upsert_volatility(currency=Currency(name="USD"), ts=pd.Series({t1: 6}))


    def test_reference_securities_frame(self):
        # create a security
        s1 = Security(name=410)
        s1.reference[KIID] = 5

        o1 = Owner(name=200)
        x = Owner.reference_frame(products=[s1], name="Owner")

        frame = pd.DataFrame(index=["410"], columns=["KIID"], data=[[5]])
        frame.index.name = "Owner"
        pdt.assert_frame_equal(x, frame)

        o1.securities.append(s1)
        pdt.assert_frame_equal(x, o1.reference_securities, check_names=False)

    def test_double_position(self):
        o = Owner(name=999, currency=Currency(name="USD"), custodian=Custodian(name="CS"))
        s = Security(name=777)
        x = pd.Series({t1.date(): 0.1})
        #s.upsert_volatility(currency=Currency(name="USD"), ts=pd.Series({t1.date(): 10}))

        o.upsert_position(s, ts=x)
        a = o.position

        o.upsert_position(s, ts=x)
        b = o.position
        #print(o.position)
        pdt.assert_frame_equal(a,b)
        #assert False

    def test_json(self):
        o = Owner(name="Peter")
        o.returns = pd.Series({t0.date(): 0.1, t1.date(): 0.0, t2.date(): -0.1})
        a = o.to_json()
        self.assertEqual(a["name"], "Peter")
        pdt.assert_series_equal(a["Nav"], pd.Series({t0: 1.10, t1: 1.10, t2: 0.99}))

    def test_volatility(self):
        o = Owner(name="Peter")
        o.volatility = pd.Series({t0.date(): 0.1, t1.date(): 0.0, t2.date(): 0.1})
        pdt.assert_series_equal(o.volatility, pd.Series({t0.date(): 0.1, t1.date(): 0.0, t2.date(): 0.1}))

