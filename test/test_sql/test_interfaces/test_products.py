import pandas as pd
import pandas.util.testing as pdt
import pytest
from sqlalchemy.exc import IntegrityError

from pyutil.mongo.mongo import mongo_client
from pyutil.sql.base import Base
from pyutil.sql.interfaces.ref import Field, FieldType, DataType
from pyutil.sql.session import session_factory
from test.test_sql.product import Product


@pytest.fixture()
def ts1():
    return pd.Series(data=[100, 200], index=[0, 1])

@pytest.fixture()
def ts2():
    return pd.Series(data=[300, 300], index=[1, 2])

@pytest.fixture()
def ts3():
    return pd.Series(data=[100, 300, 300], index=[0, 1, 2])


class TestProductInterface(object):
    def test_name(self):
        assert Product(name="A").name == "A"
        assert Product(name="A").discriminator == "product"

        # you can not change the name of a product!
        with pytest.raises(AttributeError):
            Product(name="A").name = "AA"

    def test_reference(self):
        f = Field(name="z", type=FieldType.dynamic, result=DataType.integer)
        p = Product(name="A")
        assert p.reference.get(f, 5) == 5
        assert not p.reference.get(f)

        p.reference[f] = "120"
        assert p.reference[f] == 120


    def test_duplicate(self):
        connection_str = "sqlite:///:memory:"
        s = session_factory(connection_str=connection_str, base=Base, echo=True)

        # create and commit the product A
        s.add(Product(name="A"))
        s.commit()

        # now try to add the same product again...
        s.add(Product(name="A"))
        with pytest.raises(IntegrityError):
            s.commit()

    def test_timeseries(self, ts1):
        product = Product(name="A")

        product.write(data=ts1, kind="y")
        pdt.assert_series_equal(ts1, product.read(parse=True, kind="y"))

        y = product.frame(kind="y")
        pdt.assert_series_equal(ts1, y["A"], check_names=False)

        s = [x for x in product.meta()]
        assert {"kind": "y", "name": "A"} in s


    def test_merge(self, ts1, ts2):
        product = Product(name="A")
        product.write(data=ts1, kind="x")
        product.write(data=ts2, kind="x")
        pdt.assert_series_equal(product.read(kind="x"), ts2)

    #def test_collections(self, ts1):
    #    p = Product(name="A")
    #    p.write(data=ts1, kind="xx")
    #    p.write(data=ts1, kind="yx")


    def test_lt(self):
        p1 = Product(name="A")
        p2 = Product(name="B")
        assert p1 < p2

