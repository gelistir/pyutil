# # point to a new mongo collection...
# import pandas as pd
# import pytest
#
# from pyutil.mongo.mongo import create_collection
# from test.test_sql.product import Maffay
#
#
# @pytest.fixture()
# def ts1():
#     return pd.Series(data=[100, 200], index=[0, 1])
#
# @pytest.fixture()
# def product(ts1):
#     Maffay.collection = create_collection()
#     Maffay.collection_reference = create_collection()
#     # Product._client = mongo_client()
#     p = Maffay(name="A")
#     assert p.__tablename__ == "mymodel"
#     p.reference["X"] = "Peter Maffay"
#     p.series["return"] = ts1
#     #p.series["y"] = ts1
#     #p.series["x"] = ts1
#     #p.series.write(data=ts1, key="Correlation", second="B", third="C")
#     #p.series.write(data=ts1, key="Correlation", second="C", third="D")
#     return p
#
#
# class TestPPP(object):
#     def test_name(self, product, ts1):
#         assert product.name == "A"
#         assert product.reference["X"] == "Peter Maffay"
#         assert list(product.series["return"].values) == [100, 200]
