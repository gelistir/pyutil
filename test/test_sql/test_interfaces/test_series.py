# import pandas as pd
# from sqlalchemy.ext.associationproxy import association_proxy
# from sqlalchemy.orm import relationship
#
# from pyutil.sql.interfaces.products import ProductInterface
# from pyutil.sql.interfaces.series import Series
# from test.test_sql.product import Product
#
# import pandas.util.testing as pdt
#
#
# class TestProductInterface(object):
#     def test(self):
#         p2 = Product(name="B")
#         p3 = Product(name="C")
#
#         # link a series to two products
#         s = Series(name="wurst", product2=p2, product3=p3)
#         assert not s.data
#         s.data = pd.Series([1,2,3])
#
#         pdt.assert_series_equal(s.data, pd.Series([1,2,3]))
#
#         assert s.product_2 == p2
#         assert s.product_3 == p3
#         assert s.key == (p2, p3)
#
#     def test_product(self):
#         # create an attribute price for the Product
#         Product._price = relationship(Series, uselist=False, primaryjoin=ProductInterface.join_series("price"))
#         Product.price = association_proxy("_price", "data", creator=lambda data: Series(name="price", data=data))
#
#         # create a product
#         p = Product(name="Peter")
#
#         # modify the attribute
#         p.price = pd.Series(pd.Series([1,2,3]))
#
#         pdt.assert_series_equal(p._price.data, pd.Series([1,2,3]))
#         pdt.assert_series_equal(p.price, pd.Series([1,2,3]))
#
#
