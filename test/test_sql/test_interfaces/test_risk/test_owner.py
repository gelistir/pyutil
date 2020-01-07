import pandas as pd
import pandas.util.testing as pdt
import pytest

#from pyutil.mongo.mongo import create_collection
from pyutil.sql.interfaces.risk.custodian import CurrencyMongo, CustodianMongo
from pyutil.sql.interfaces.risk.owner import OwnerMongo
from pyutil.sql.interfaces.risk.security import SecurityMongo
#from pyutil.sql.product import Product
from pyutil.timeseries.merge import merge

from test.config import mongo_client

t0 = pd.Timestamp("1978-11-15")
t1 = pd.Timestamp("1978-11-16")
t2 = pd.Timestamp("1978-11-18")

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


@pytest.fixture()
def ts1():
    return pd.Series(data=[100, 200], index=[0, 1])

@pytest.fixture()
def ts2():
    return pd.Series(data=[300, 300], index=[1, 2])

@pytest.fixture()
def ts3():
    return pd.Series(data=[100, 300, 300], index=[0, 1, 2])


@pytest.fixture()
def owner(mongo_client):
    c = CurrencyMongo(name="USD")
    c.save()
    return OwnerMongo(name=100, currency=c, fullname="Peter Maffay")

#@pytest.fixture()
#def kiid():
#    return Field(name="KIID", result=DataType.integer, type=FieldType.other)


class TestOwner(object):
    def test_position(self):
        #Product.mongo_database = mongo
        #assert Security.mongo_database
        #assert Owner.mongo_database

        # create a security
        s1 = SecurityMongo(name="123", fullname="A")

        s1.reference["KIID"] = 5
        assert s1.reference["KIID"] == 5

        # create a 2nd security
        s2 = SecurityMongo(name="211", fullname="B")
        s2.reference["KIID"] = 7
        assert s2.reference["KIID"] == 7

        #assert owner.custodians == set([])
        #assert owner.securities == set([])

        # one owner can have multiple custodians
        c1 = CustodianMongo(name="UBS")
        c2 = CustodianMongo(name="CS")

        # # update a position in a security, you have to go through an owner! Position without an owner wouldn't make sense
        # owner.upsert_position(security=s1, custodian=c1, ts=pd.Series({t1: 0.1, t2: 0.4}))
        # owner.upsert_position(security=s2, custodian=c2, ts=pd.Series({t1: 0.5, t2: 0.5}))
        #
        # assert owner.securities == {s1, s2}
        # assert owner.custodians == {c1, c2}
        #
        # pdt.assert_series_equal(owner.position(s1, c1), pd.Series({t1: 0.1, t2: 0.4}))
        # pdt.assert_frame_equal(pd.DataFrame(index=[s1, s2], columns=["Entity ID","KIID", "Name"], data=[[123, 5, "A"], [211, 7, "B"]]),
        #                        owner.reference_securities, check_names=False, check_dtype=False)
        #
        # #s1.upsert_volatility(currency=Currency(name="USD"), ts=pd.Series({t1: 5, t2: 6.0}))
        # #s2.upsert_volatility(currency=Currency(name="USD"), ts=pd.Series({t1: 6}))
        #
        # x = pd.DataFrame(data=[[s1, c1, t1, 0.1, 123, 5, "A", 5.0],
        #                        [s1, c1, t2, 0.4, 123, 5, "A", 6.0],
        #                        [s2, c2, t1, 0.5, 211, 7, "B", 6.0],
        #                        [s2, c2, t2, 0.5, 211, 7, "B", np.nan]],
        #                  columns=["Security", "Custodian", "Date", "Position", "Entity ID", "KIID", "Name", "Volatility"])
        #
        # x = x.set_index(keys=["Security", "Custodian", "Date"])
        #
        # #pdt.assert_frame_equal(x, owner.position_reference, check_dtype=False)
        #
        # owner.upsert_volatility(ts=pd.Series([10, 20]))
        #
        # owner.flush()
        #
        # assert owner.position_frame.empty
        # assert owner.position_reference.empty
        #
        # frame = Owner.frame(owners=[owner])
        # pdt.assert_frame_equal(frame, pd.DataFrame(index=pd.Index([owner], name="Security"), columns=["Currency", "Entity ID", "Name"], data=[["USD",100,"Peter Maffay"]]), check_dtype=False)

    def test_returns(self, ts1, ts2, ts3):
        #Product.mongo_database = mongo

        o = OwnerMongo(name="222")
        #assert Owner.mongo_database

        o.returns = ts1 #.write(data=ts1, key="RETURN")
        pdt.assert_series_equal(ts1, o.returns)

        o.returns = merge(new=ts2, old=ts1)
        pdt.assert_series_equal(merge(new=ts2, old=ts1), ts3)

        #"RETURN"))
        #ts=pd.Series(data=[250, 300], index=[1, 2]))
        pdt.assert_series_equal(ts3, o.returns)

    #def test_volatility(self):
    #    o = Owner(name="222")
    #    x = o.upsert_volatility(ts=pd.Series([100, 200]))
    #    pdt.assert_series_equal(x, pd.Series([100, 200]))

    #    x = o.upsert_volatility(ts=pd.Series(data=[250, 300], index=[1, 2]))
    #    pdt.assert_series_equal(x, pd.Series(data=[100, 250, 300], index=[0, 1, 2]))

    #def test_currency(self, owner):
    #    owner.currency = Currency(name="CHFX")
    #    assert owner.currency == Currency(name="CHFX")

    #def test_custodian(self, owner):
    #    owner.custodian = Custodian(name="UBS")
    #    assert owner.custodian == Custodian(name="UBS")

    def test_name(self, mongo_client):
        #Product.mongo_database = mongo
        #assert Owner.mongo_database
        #assert Security.mongo_database
        c = CurrencyMongo(name="CHF")
        c.save()

        o = OwnerMongo(name="222", fullname="Peter Maffay", currency=c)

        assert o.name == "222"
        assert o.currency == c

        #assert str(o) == "Owner(222: Peter Maffay, CHF)"
        #assert o.currency == Currency(name="CHF")

        #f = Field(name="z", type=FieldType.dynamic, result=DataType.integer)
        o.reference["z"] = 20
        #o.reference[f] = 20

        frame = pd.DataFrame(index=["z","Currency", "Entity ID", "Name"], columns=["222"],
                             data=[20, "CHF", "222", "Peter Maffay"]).transpose()
        frame.index.name = "owner"
        pdt.assert_frame_equal(OwnerMongo.reference_frame(owners=[o], f=lambda x: x.name)[frame.keys()], frame, check_dtype=False)

    #def test_json(self):
    #    o = Owner(name="Peter")
    #    o._returns = pd.Series({t0.date(): 0.1, t1.date(): 0.0, t2.date(): -0.1})
    #    a = o.to_json()
    #    assert a["name"] == "Peter"
    #    pdt.assert_series_equal(a["Nav"], pd.Series({t0: 1.10, t1: 1.10, t2: 0.99}))

    # def test_position_update(self):
    #     o = Owner(name="Thomas")
    #     c = Custodian(name="Hans")
    #     s = Security(name=123)
    #     o.upsert_position(security=s, custodian=c, ts=pd.Series([10, 20, 30]))
    #     pdt.assert_series_equal(o._position[(s, c)], pd.Series([10, 20, 30]))

