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

# point to a new mongo collection...
ProductInterface.__collection__ = create_collection()


class TestSecurity(object):
    def test_name(self):
        s = Security(name=100)
        assert s.name == "100"
        assert s.discriminator == "security"
        assert str(s) == "Security(100: None)"

        assert s.bloomberg_scaling == 1
        assert not s.bloomberg_ticker

    def test_write_volatility(self, ts):
        s = Security(name=100)
        s.write(kind="VOLATILITY", data=ts, currency="USD")
        x = s.read(parse=True, kind="VOLATILITY")
        pdt.assert_series_equal(x, ts, check_names=False)

    def test_frame(self, ts):
        s1 = Security(name="A")
        s2 = Security(name="B")
        s3 = Security(name="C")
        s1.write(kind="PRICE2", data=ts)
        s2.write(kind="PRICE2", data=ts)
        s3.write(kind="PRICE2", data=None)
        frame = Security.frame(kind="PRICE2")
        pdt.assert_series_equal(ts, frame["A"], check_names=False)
        pdt.assert_series_equal(ts, frame["B"], check_names=False)
        assert set(frame.keys()) == {"A", "B"}

    def test_prices_1(self, ts):
        security = Security(name="Thomas")
        security.write(data=ts, kind="PX_OPEN")
        pdt.assert_series_equal(security.read(kind="PX_OPEN"), ts)

        frame = Security.frame(kind="PX_OPEN")
        pdt.assert_series_equal(frame["Thomas"], ts, check_names=False)

    def test_prices_2(self, ts):
        security = Security(name="Peter Maffay")
        security.upsert_price(data=ts)
        pdt.assert_series_equal(security.price, ts)

        security.price = ts
        pdt.assert_series_equal(security.price, ts)

        security.upsert_price(data=2*ts.tail(100))
        pdt.assert_series_equal(security.price.tail(100), 2*ts.tail(100))

        assert security.last == ts.last_valid_index()

    def test_prices_3(self, ts):
        s1 = Security(name="A")
        s1.price = ts

        s2 = Security(name="B")
        s2.price = 2*ts

        f = Security.prices([s1, s2])
        pdt.assert_series_equal(f["A"], ts, check_names=False)
        pdt.assert_series_equal(f["B"], 2*ts, check_names=False)

