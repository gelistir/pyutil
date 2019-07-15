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
def security(ts):
    # point to a new mongo collection...
    Security.__collection__ = create_collection()
    Security.__collection_reference__ = create_collection()

    s = Security(name=100, fullname="Peter Maffay")
    s["Bloomberg Ticker"] = "IBM US Equity"
    s.write(key="PRICE", data=ts)
    return s


class TestSecurity(object):
    def test_name(self, security):
        # s = Security(name=100)
        assert security.name == "100"
        assert str(security) == "Security(100: None)"

        #assert security.bloomberg_scaling == 1
        #assert security.bloomberg_ticker == "IBM US Equity"

    def test_write_volatility(self, ts, security):
        # s = Security(name=100)
        security.write(key="VOLATILITY", data=ts, currency="USD")
        x = security.read(key="VOLATILITY")
        pdt.assert_series_equal(x, ts, check_names=False)

    def test_prices(self, security, ts):
        s0 = security

        s1 = Security(name="A")
        s1.write(data=ts, key="PRICE")

        s2 = Security(name="B")
        s2.write(data=2*ts, key="PRICE")

        f = Security.pandas_frame(products=[s1, s2], key="PRICE")
        pdt.assert_series_equal(f["A"], ts, check_names=False)
        pdt.assert_series_equal(f["B"], 2 * ts, check_names=False)

    def test_reference(self, security):
        frame1 = Security.reference_frame(products=[security], f=lambda x: x.name)
        frame2 = pd.DataFrame(index=["100"], columns=["Bloomberg Ticker", "fullname"], data=[["IBM US Equity", "Peter Maffay"]])
        pdt.assert_frame_equal(frame1, frame2, check_names=False)

    def test_health(self, security):
        missing = security.health(keys=["Bloomberg Ticker", "AAA"])
        assert missing == {"AAA"}