import pandas as pd
import pandas.util.testing as pdt
import pytest

from pyutil.sql.interfaces.risk.security import SecurityMongo, SecurityVolatility
from pyutil.sql.interfaces.risk.custodian import CurrencyMongo
from test.config import read, mongo_client

t0 = pd.Timestamp("1978-11-15")
t1 = pd.Timestamp("1978-11-16")


@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)


@pytest.fixture()
def security(ts):
    # point to a new mongo collection... with a random name...
    #Security.collection = create_collection()
    #Security.collection_reference = create_collection()

    s = SecurityMongo(name="100", fullname="Peter Maffay")
    #Security.mongo_database = mongo
    s.reference["Bloomberg Ticker"] = "IBM US Equity"
    s.price = ts
    return s


class TestSecurity(object):
    def test_name(self, security):
        # s = Security(name=100)
        assert security.name == "100"
        #assert str(security) == "Security(100: None)"
        assert security.fullname == "Peter Maffay"
        #assert security.bloomberg_scaling == 1
        #assert security.bloomberg_ticker == "IBM US Equity"

    def test_write_volatility(self, mongo_client, ts, security):
        # s = Security(name=100)
        c = CurrencyMongo(name="USD")
        c.save()

        security.save()

        sv = SecurityVolatility(currency=c, security=security)
        sv.volatility = ts
        #security.series.write(key="VOLATILITY", data=ts, currency="USD")
        #x = security.series["VOLATILITY"]
        pdt.assert_series_equal(sv.volatility, ts, check_names=False)

    def test_prices(self, security, ts):
        s0 = security

        s1 = SecurityMongo(name="A")
        s1.price = ts

        s2 = SecurityMongo(name="B")
        s2.price = 2*ts

        f = SecurityMongo.pandas_frame(products=[s0, s1, s2], key="price", f=lambda x: x.name)
        pdt.assert_series_equal(f["A"], ts, check_names=False)
        pdt.assert_series_equal(f["B"], 2 * ts, check_names=False)

    def test_reference(self, security):
        frame1 = SecurityMongo.reference_frame(securities=[security], f=lambda x: x.name)
        frame2 = pd.DataFrame(index=["100"], columns=["Bloomberg Ticker", "fullname"], data=[["IBM US Equity", "Peter Maffay"]])
        pdt.assert_frame_equal(frame1, frame2, check_names=False)
