import pandas as pd
import pandas.util.testing as pdt
import pytest

from pyutil.mongo.mongo import create_collection
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.risk.security import Security
from test.config import read

t0 = pd.Timestamp("1978-11-15")
t1 = pd.Timestamp("1978-11-16")


@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)


@pytest.fixture()
def security():
    s = Security(name=100)
    s["Bloomberg Ticker"] = "IBM US Equity"
    return s


# point to a new mongo collection...
ProductInterface.__collection__ = create_collection()
ProductInterface.__collection_reference__ = create_collection()


class TestSecurity(object):
    def test_name(self, security):
        # s = Security(name=100)
        assert security.name == "100"
        assert str(security) == "Security(100: None)"

        assert security.bloomberg_scaling == 1
        assert security.bloomberg_ticker == "IBM US Equity"

    def test_write_volatility(self, ts, security):
        # s = Security(name=100)
        security.write(key="VOLATILITY", data=ts, currency="USD")
        x = security.read(key="VOLATILITY")
        pdt.assert_series_equal(x, ts, check_names=False)

    def test_frame(self, ts):
        s1 = Security(name="A")
        s2 = Security(name="B")
        s3 = Security(name="C")
        s1.write(key="PRICE", data=ts)
        s2.write(key="PRICE", data=ts)
        s3.write(key="PRICE", data=None)
        frame = Security.prices(securities=[s1, s2, s3])
        print(frame)

        pdt.assert_series_equal(ts, s1.read(key="PRICE"), check_names=False)
        pdt.assert_series_equal(ts, s2.read(key="PRICE"), check_names=False)
        pdt.assert_series_equal(ts, frame["A"], check_names=False)
        pdt.assert_series_equal(ts, frame["B"], check_names=False)
        assert set(frame.keys()) == {"A", "B"}

    #def test_prices_1(self, ts):
    #    security = Security(name="Thomas")
    #    security.write(data=ts, key="PX_OPEN")
    #    pdt.assert_series_equal(security.read(key="PX_OPEN"), ts)

    #    frame = Security.pandas_frame(products=[security], key="PX_OPEN")
    #    pdt.assert_series_equal(frame["Thomas"], ts, check_names=False)

    def test_prices(self, ts):
        s1 = Security(name="A")
        s1.write(data=ts, key="PRICE")

        s2 = Security(name="B")
        s2.write(data=2*ts, key="PRICE")

        f = Security.pandas_frame(products=[s1, s2], key="PRICE")
        pdt.assert_series_equal(f["A"], ts, check_names=False)
        pdt.assert_series_equal(f["B"], 2 * ts, check_names=False)
