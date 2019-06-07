import pandas as pd
import pandas.util.testing as pdt
from sqlalchemy.exc import IntegrityError

from pyutil.sql.interfaces.ref import Field, FieldType, DataType
from pyutil.timeseries.merge import merge
from test.test_sql.product import Product
from pyutil.sql.session import session_factory
from pyutil.sql.base import Base

t0 = pd.Timestamp("2010-05-14")
t1 = pd.Timestamp("2010-05-15")


import pytest


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
        #assert p.get_reference(field="z", default=5) == 5
        #assert p.get_reference(field="zzz", default=5) == 5

        p.reference[f] = "120"
        assert p.reference[f] == 120

    def test_timeseries(self):
        a = pd.Series(index=[0, 1], data=[4, 5])
        b = pd.Series(index=[1, 3], data=[10, 12])
        c = merge(old=a, new=b)

        pdt.assert_series_equal(pd.Series(index=[0, 1, 3], data=[4, 10, 12]), c)

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

