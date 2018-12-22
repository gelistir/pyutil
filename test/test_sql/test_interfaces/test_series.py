from unittest import TestCase

import pandas as pd
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.series import Series
from test.test_sql.product import Product

import pandas.util.testing as pdt


class TestProductInterface(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p1 = Product(name="A")
        cls.p2 = Product(name="B")
        cls.p3 = Product(name="C")

    def test(self):
        s = Series(name="wurst", product2=self.p2, product3=self.p3)
        self.assertIsNone(s.data)
        s.data = pd.Series([1,2,3])

        pdt.assert_series_equal(s.data, pd.Series([1,2,3]))

        self.assertEqual(s.product_2, self.p2)
        self.assertEqual(s.product_3, self.p3)
        self.assertEqual(s.key, (self.p2, self.p3))

    def test_product(self):
        Product._price = relationship(Series, uselist=False, primaryjoin=ProductInterface.join_series("price"))
        Product.price = association_proxy("_price", "data", creator=lambda data: Series(name="price", data=data))

        p = Product(name="Peter")
        p.price = pd.Series(pd.Series([1,2,3]))

        pdt.assert_series_equal(p._price.data, pd.Series([1,2,3]))
        pdt.assert_series_equal(p.price, pd.Series([1,2,3]))
        #self.assertEqual(Product._price.product1, p)

