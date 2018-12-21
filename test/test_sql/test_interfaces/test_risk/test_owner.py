import unittest

import pandas as pd
import numpy as np

import pandas.util.testing as pdt

from pyutil.sql.interfaces.risk.custodian import Custodian, Currency
from pyutil.sql.interfaces.risk.owner import Owner
from pyutil.sql.interfaces.risk.security import Security
from pyutil.sql.interfaces.ref import Field, DataType, FieldType

t0 = pd.Timestamp("1978-11-15")
t1 = pd.Timestamp("1978-11-16")
t2 = pd.Timestamp("1978-11-18")

#KIID = FIELDSSECURITY["Lobnek KIID"]
#CUSTODIAN = FIELDSOWNER["15. Custodian Name"]
#NAME = FIELDSOWNER["name"]

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class TestOwner(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # create an owner
        cls.o1 = Owner(name=100)
        #cls.field = Field(name="KIID", )
        cls.kiid = Field(name="KIID", result=DataType.integer, type=FieldType.other)

    def test_position(self):
        o = Owner(name="103", currency=Currency(name="USD"))

        # create a security
        s1 = Security(name="123", fullname="A")
        s1.reference[self.kiid] = 5

        s2 = Security(name="211", fullname="B")
        s2.reference[self.kiid] = 7

        c1 = Custodian(name="UBS")
        c2 = Custodian(name="CS")

        # update a position in a security, you have to go through an owner! Position without an owner wouldn't make sense
        o._position[(s1, c1)] = pd.Series({t1: 0.1, t2: 0.4})
        o._position[(s2, c2)] = pd.Series({t1: 0.5, t2: 0.5})

        print(o.reference_securities)
        #x.index = [a.name for a in x.index]

        #assert False

        self.assertSetEqual(o.securities, {s1, s2})
        pdt.assert_frame_equal(pd.DataFrame(index=[s1, s2], columns=["Entity ID","KIID", "Name"], data=[[123, 5, "A"], [211, 7, "B"]]),
                               o.reference_securities, check_names=False, check_dtype=False)

        s1.upsert_volatility(currency=Currency(name="USD"), ts=pd.Series({t1: 5, t2: 6.0}))
        s2.upsert_volatility(currency=Currency(name="USD"), ts=pd.Series({t1: 6}))

        x = pd.DataFrame(data=[[s1, c1, t1, 0.1, 123, 5, "A", 5.0],
                               [s1, c1, t2, 0.4, 123, 5, "A", 6.0],
                               [s2, c2, t1, 0.5, 211, 7, "B", 6.0],
                               [s2, c2, t2, 0.5, 211, 7, "B", np.nan]],
                         columns=["Security", "Custodian", "Date", "Position", "Entity ID", "KIID", "Name", "Volatility"])

        x = x.set_index(keys=["Security", "Custodian", "Date"])

        print(x)

        pdt.assert_frame_equal(x, o.position_reference, check_dtype=False)

        o.upsert_volatility(ts=pd.Series([10, 20]))

        o.flush()
        self.assertTrue(o.position_frame.empty)
        self.assertTrue(o.position_reference.empty)




    def test_returns(self):
        o = Owner(name="222")
        x = o.upsert_returns(ts=pd.Series(data=[100, 200], index=[0, 1]))
        pdt.assert_series_equal(x, pd.Series(data=[100, 200], index=[0, 1]))

        x = o.upsert_returns(ts=pd.Series(data=[250, 300], index=[1, 2]))
        pdt.assert_series_equal(x, pd.Series(data=[100, 250, 300], index=[0, 1, 2]))

    def test_volatility(self):
        o = Owner(name="222")
        x = o.upsert_volatility(ts=pd.Series([100, 200]))
        pdt.assert_series_equal(x, pd.Series([100, 200]))

        x = o.upsert_volatility(ts=pd.Series(data=[250, 300], index=[1, 2]))
        pdt.assert_series_equal(x, pd.Series(data=[100, 250, 300], index=[0, 1, 2]))

    def test_currency(self):
        o = Owner(name="222")
        o.currency = Currency(name="CHFX")
        self.assertEqual(o.currency, Currency(name="CHFX"))

    def test_custodian(self):
        o = Owner(name="222")
        o.custodian = Custodian(name="UBS")
        self.assertEqual(o.custodian, Custodian(name="UBS"))

    def test_securities(self):
        o = Owner(name="222")
        # The owner has no securities, hence empty set...
        self.assertSetEqual(o.securities, set([]))

    def test_name(self):
        o = Owner(name="222")
        self.assertEqual(o.name, "222")
        self.assertEqual(str(o), "Owner(222: None)")

    def test_double_position(self):
        o = Owner(name=999, currency=Currency(name="USD"))
        s = Security(name=777)
        x = pd.Series({t1.date(): 0.1})
        s._vola[Currency(name="USD")] = pd.Series({t1.date(): 10})

        # o.upsert_position(s, ts=x)
        o._position[(s, Custodian(name="UBS"))] = x
        a = o._position

        # o.upsert_position(s, ts=x)
        # b = o.position
        # print(o.position)
        # pdt.assert_frame_equal(a,b)
        # assert False

    def test_json(self):
        o = Owner(name="Peter")
        o._returns = pd.Series({t0.date(): 0.1, t1.date(): 0.0, t2.date(): -0.1})
        a = o.to_json()
        self.assertEqual(a["name"], "Peter")
        pdt.assert_series_equal(a["Nav"], pd.Series({t0: 1.10, t1: 1.10, t2: 0.99}))

    def test_position_update(self):
        o = Owner(name="Thomas")
        c = Custodian(name="Hans")
        s = Security(name=123)
        o.upsert_position(security=s, custodian=c, ts=pd.Series([10, 20, 30]))
        pdt.assert_series_equal(o._position[(s, c)], pd.Series([10, 20, 30]))
