import unittest
from collections import OrderedDict

import pandas as pd

from pyutil.sql.interfaces.risk.custodian import Custodian
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


def date2str(x):
    return x.strftime("%Y-%m-%d")

class TestOwner(unittest.TestCase):
    def test_name(self):
        o = Owner(name=100, currency=Currency(name="USD"))
        self.assertEqual(o.currency, Currency(name="USD"))
        self.assertEqual(o.name, "100")


    def test_currency(self):
        o = Owner(name="Peter")
        currency = Currency(name="CHF")
        o.currency = currency
        self.assertEqual(o.currency, currency)

    def test_custodian(self):
        custodian=Custodian(name="UBS Geneva")
        o = Owner(name="Peter", currency=Currency(name="USD"))
        o.custodian = custodian
        self.assertEqual(o.custodian, custodian)


    def test_no_position(self):
        o = Owner(name="Peter", currency=Currency(name="USD"))
        # we have no position (yet)!
        self.assertIsNone(o.current_position)
        self.assertTrue(o.nav.empty)


    def test_str(self):
        o = Owner(name="Peter", currency=Currency(name="USD"))
        # You need to define the Field "Name" to get rid of the None
        self.assertEqual(str(o), "Owner(Peter: None)")

        o.reference[NAME] = "Maffay"
        self.assertEqual(str(o), "Owner(Peter: Maffay)")

    def test_return(self):
        o = Owner(name=100, currency=Currency(name="USD"))
        o.returns_upsert(ts={t1: 0.1, t2: 0.2})
        pdt.assert_series_equal(o.returns, pd.Series({t1: 0.1, t2: 0.2}))
        # Note that for the Nav we introduce a new point in time on the fly!?
        pdt.assert_series_equal(o.nav, pd.Series(index=[t0, t1, t2], data=[1.0, 1.1, 1.32]))

    def test_volatility(self):
        o = Owner(name="100", currency=Currency(name="USD"))
        o.volatility_upsert(ts={t1: 0.1, t2: 0.3})
        pdt.assert_series_equal(o.volatility, pd.Series({t1: 0.1, t2: 0.3}))

    def test_add_security(self):
        o = Owner(name="100", currency=Currency(name="USD"))

        # create a security
        s1 = Security(name="123")
        s1.reference[KIID] = "5"

        # note that the security is not linked to the owner yet
        self.assertTrue(o.reference_securities.empty)
        self.assertListEqual(o.securities, [])

        # appending securities can be done explicitly as demonstrated here or by defining a position in the security
        o.securities.append(s1)
        pdt.assert_frame_equal(o.reference_securities,
                               pd.DataFrame(index=["123"], columns=["KIID"], data=[[5]]), check_dtype=False)
        self.assertListEqual(o.securities, [s1])

        # let's remove the security we have just added!
        o.securities.pop(0)
        self.assertTrue(o.reference_securities.empty)

    def test_position(self):
        o = Owner(name="100", currency=Currency(name="USD"))
        c = Custodian(name="UBS")

        # create a security
        s1 = Security(name="123")
        s1.reference[KIID] = 5

        # update a position in a security, you have to go through an owner! Position without an owner wouldn't make sense
        o.position_upsert(security=s1, custodian=c, ts={t1: 0.1, t2: 0.4})

        pdt.assert_frame_equal(o.position(sum=False),
                               pd.DataFrame(columns=pd.Index([date2str(t1), date2str(t2)]), index=["123"],
                                            data=[[0.1, 0.4]]), check_names=False)

        pdt.assert_frame_equal(o.position(sum=False, tail=1),
                               pd.DataFrame(columns=pd.Index([date2str(t2)]), index=["123"],
                                            data=[[0.4]]), check_names=False)


        pdt.assert_frame_equal(o.position(sum=True, tail=1),
                               pd.DataFrame(columns=pd.Index([date2str(t2)]), index=["123", "Sum"],
                                            data=[[0.4],[0.4]]), check_names=False)

        pdt.assert_series_equal(o.current_position, pd.Series({"123": 0.4}), check_names=False)

        print(o.position_by())
        pdt.assert_frame_equal(o.position_by(tail=1), pd.DataFrame(index=["123"], columns=[date2str(t2), "KIID"],
                                                              data=[[0.4, 5]]), check_dtype=False, check_names=False)

        print(o.position_by(index_col="KIID"))

        pdt.assert_frame_equal(o.position_by(index_col="KIID", tail=1), pd.DataFrame(index=[5], columns=pd.Index([date2str(t2)]), data=[[0.4]]), check_names=False)

        pdt.assert_frame_equal(o.position_by(sum=True), pd.DataFrame(index=["123", "Sum"], columns=[date2str(t1), date2str(t2), "KIID"],
                                                             data=[[0.1, 0.4, 5],[0.1,0.4,None]]), check_dtype=False, check_names=False)

        pdt.assert_frame_equal(o.position_by(index_col="KIID", sum=True, tail=1), pd.DataFrame(index=[5, "Sum"], columns=pd.Index([date2str(t2)]), data=[[0.4], [0.4]]), check_names=False)



    def test_volatility(self):
        o = Owner(name=100, currency=Currency(name="USD"))
        c = Custodian(name="UBS")
        # create a security
        s1 = Security(name=123)
        s1.reference[KIID] = 5

        # update the position in security s1
        o.position_upsert(security=s1, custodian=c, ts={t1: 0.1, t2: 0.4})

        # update the volatility, note that you can update the volatility even after the security has been added to the owner
        s1.volatility_upsert(currency=o.currency, ts={t1: 2.5, t2: 2.5})


        pdt.assert_frame_equal(o.vola_securities, pd.DataFrame(columns=pd.Index([date2str(t1), date2str(t2)]), index=["123"], data=[[2.5, 2.5]]))

        pdt.assert_frame_equal(o.vola_weighted(sum=False), pd.DataFrame(columns=pd.Index([date2str(t1), date2str(t2)]), index=["123"], data=[[0.25, 1.0]]), check_names=False)


        pdt.assert_frame_equal(o.vola_weighted_by(), pd.DataFrame(index=["123"], columns=[date2str(t1), date2str(t2), "KIID"],
                                                                   data=[[0.25, 1.0, 5]]), check_dtype=False, check_names=False)
        pdt.assert_frame_equal(o.vola_weighted_by(index_col="KIID"),
                               pd.DataFrame(index=[5], columns=pd.Index([date2str(t1), date2str(t2)]), data=[[0.25, 1.0]]),
                               check_names=False)

    def test_owners(self):
        o1 = Owner(name='100', currency=Currency(name="USD"))
        o2 = Owner(name='1300', currency=Currency(name="USD"))
        s1 = Security(name="123")
        c = Custodian(name="UBS")
        o1.reference[NAME] = "Peter"
        o2.reference[NAME] = "Maffay"
        #pdt.assert_frame_equal(pd.DataFrame(index=['100', '1300'], columns=["Name"], data=[["Peter"],["Maffay"]]), o.reference, check_names=False)

        #print(o.to_html_dict())
        #self.assertDictEqual(o.to_html_dict(), {'columns': ['Entity ID', 'Name'], 'data': [OrderedDict([('Entity ID', '100'), ('Name', 'Peter')]), OrderedDict([('Entity ID', '1300'), ('Name', 'Maffay')])]})

        o1.returns_upsert(ts={t1: 0.1, t2: 0.4})

        #pdt.assert_frame_equal(o.returns, pd.DataFrame(index=[t1,t2], columns=["Peter"], data=[[0.1],[0.4]]), check_names=False)

        # update the position in security s1
        o1.position_upsert(security=s1, custodian=c, ts={t1: 0.1, t2: 0.4})
        o1.volatility_upsert(ts={t1: 0.1, t2: 0.4})

        #index = pd.MultiIndex.from_tuples(tuples=[("Peter","123", date2str(t1)), ("Peter", "123", date2str(t2))], names=("Owner","Asset","Date"))
        #frame = pd.DataFrame(index=index, columns=["Weight"], data=[[0.1],[0.4]])
        #pdt.assert_frame_equal(o.positions, frame)

        #frame = pd.DataFrame(index=["Peter"], columns=[t1, t2], data=[[0.1, 0.4]])
        #pdt.assert_frame_equal(o.volatility, frame, check_names=False)


    def test_kiid(self):
        o = Owner(name='100', currency=Currency(name="USD"))
        o.reference[NAME] = "Peter"
        c = Custodian(name="UBS")
        # create a security
        s1 = Security(name="123")
        s1.reference[NAME] = "Maffay"
        s1.reference[KIID] = 5

        # update the position in security s1
        o.position_upsert(security=s1, custodian=c, ts={t1: 0.1, t2: 0.4})

        pdt.assert_series_equal(o.kiid, pd.Series(index=["123"], data=[5]))
        pdt.assert_frame_equal(o.kiid_weighted(sum=False), pd.DataFrame(index=["123"], columns=pd.Index([date2str(t1), date2str(t2)]), data=[[0.5, 2.0]]), check_names=False)
        pdt.assert_frame_equal(o.kiid_weighted(sum=True), pd.DataFrame(index=["123", "Sum"], columns=pd.Index([date2str(t1), date2str(t2)]), data=[[0.5, 2.0], [0.5,2.0]]), check_names=False)

        frame = pd.DataFrame(index=["Maffay"], columns=[date2str(t1), date2str(t2)], data=[[0.5, 2.0]])
        pdt.assert_frame_equal(o.kiid_weighted_by(index_col="Name"), frame, check_names=False)


    #def test_html_do_dict(self):
    #    o = Owner(name='100', currency=Currency(name="USD"))
    #    o.returns_upsert(ts={t1: 0.1, t2: 0.4})
    #    o.reference[NAME] = "Peter"

        #self.assertDictEqual(o.to_html_dict(), {'nav': [[279936000000, 1.0], [280022400000, 1.1], [280195200000, 1.54]],
        #                                        'drawdown': [[279936000000, 0.0], [280022400000, 0.0], [280195200000, 0.0]],
        #                                        'volatility': [], 'name': 'Peter', 'weights': {'columns': ['Asset'], 'data': []}})

