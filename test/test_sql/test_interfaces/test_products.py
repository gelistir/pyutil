import pandas as pd
import pandas.util.testing as pdt
import pytest

from pyutil.sql.interfaces.symbols.symbol import Symbol


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
    Symbol.refresh_mongo()
    # Product._client = mongo_client()
    p = Symbol(name="A")
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

        assert Symbol.collection
        assert Symbol.collection_reference

    def test_timeseries(self, product, ts1):
        pdt.assert_series_equal(ts1, product.series["y"])

    def test_merge(self, product, ts2):
        # product.series["x"] = ts1
        product.series["x"] = ts2
        pdt.assert_series_equal(product.series["x"], ts2)
        frame = Symbol.pandas_frame(products=[product], key="x")
        pdt.assert_series_equal(frame[product], ts2, check_names=False)

    def test_lt(self):
        p1 = Symbol(name="A")
        p2 = Symbol(name="B")
        assert p1 < p2

    def test_repr(self):
        p1 = Symbol(name="A")
        assert str(p1) == "A"
        #p2 = Symbol(name="B")
        #assert p1 < p2

    def test_frame(self, product, ts1):
        # add some extra product but without timeseries
        frame = Symbol.pandas_frame(key="y", products=[product], f=lambda x: x.name)
        pdt.assert_series_equal(frame["A"], ts1, check_names=False)

    def test_series(self, product):
        for a in product.series.keys(key="Correlation"):
            assert isinstance(a, dict)
            assert "second" in a
            assert "third" in a
            assert a["key"] == "Correlation"
            assert a["name"] == "A"

        assert len([a for a in product.series]) == 4

    def test_items(self, product, ts1):
        for a, b in product.series.items(key="Correlation"):
            assert isinstance(a, dict)
            assert "second" in a
            assert "third" in a
            assert a["key"] == "Correlation"
            assert a["name"] == "A"
            pdt.assert_series_equal(b, ts1)


