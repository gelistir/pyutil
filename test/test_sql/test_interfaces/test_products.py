import pandas as pd
import pandas.util.testing as pdt
import pytest

# from pyutil.mongo.mongo import create_collection
from pyutil.mongo.mongo import mongo_client, create_collection
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


# point to a new mongo collection...
@pytest.fixture()
def product(ts1):
    Product.__collection__ = create_collection()
    Product.__collection_reference__ = create_collection()
    # Product._client = mongo_client()
    p = Product(name="A")
    p.series["y"] = ts1
    p.series["x"] = ts1
    p.series.write(data=ts1, key="Correlation", second="B", third="C")
    p.series.write(data=ts1, key="Correlation", second="C", third="D")
    return p


class TestProductInterface(object):
    def test_name(self, product):
        assert product.name == "A"

        # you can not change the name of a product!
        with pytest.raises(AttributeError):
            product.name = "AA"

        # assert Product.__collection__
        # assert Product.__collection_reference__

    def test_timeseries(self, product, ts1):
        pdt.assert_series_equal(ts1, product.series["y"])

    def test_merge(self, product, ts2):
        # product.series["x"] = ts1
        product.series["x"] = ts2
        pdt.assert_series_equal(product.series["x"], ts2)
        frame = Product.pandas_frame(products=[product], key="x")
        pdt.assert_series_equal(frame[product], ts2, check_names=False)

    def test_lt(self):
        p1 = Product(name="A")
        p2 = Product(name="B")
        assert p1 < p2

    def test_frame(self, product, ts1):
        # add some extra product but without timeseries
        frame = Product.pandas_frame(key="y", products=[product], f=lambda x: x.name)
        pdt.assert_series_equal(frame["A"], ts1, check_names=False)

    def test_series(self, product):
        assert product.series.keys() == {"x", "y", "Correlation"}

    def test_items(self, product, ts1):
        for a, b in product.series.items(key="Correlation"):
            assert isinstance(a, dict)
            assert "second" in a
            assert "third" in a
            assert a["key"] == "Correlation"
            assert a["name"] == "A"
            pdt.assert_series_equal(b, ts1)

