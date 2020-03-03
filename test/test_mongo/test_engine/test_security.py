import pandas as pd
import pandas.util.testing as pdt
import pytest

from pyutil.mongo.engine.security import Security, SecurityVolatility
from pyutil.mongo.engine.currency import Currency
from test.config import mongo, read

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

    s = Security(name="100", fullname="Peter Maffay", price=ts)
    #Security.mongo_database = mongo
    s.reference["Bloomberg Ticker"] = "IBM US Equity"
    #s.price = ts
    return s


def test_name(security):
    # s = Security(name=100)
    assert security.name == "100"
    #assert str(security) == "Security(100: None)"
    assert security.fullname == "Peter Maffay"
    #assert security.bloomberg_scaling == 1
    #assert security.bloomberg_ticker == "IBM US Equity"

def test_write_volatility(mongo, ts, security):
    with mongo as m:
        # s = Security(name=100)
        c = Currency(name="USD").save()
        security.save()

        sv = SecurityVolatility(currency=c, security=security, volatility=ts)
        #sv.volatility = ts
        #security.series.write(key="VOLATILITY", data=ts, currency="USD")
        #x = security.series["VOLATILITY"]
        pdt.assert_series_equal(sv.volatility, ts, check_names=False)

def test_prices(security, ts):
    s0 = security

    s1 = Security(name="A", price=ts)
    #s1.price = ts

    s2 = Security(name="B", price=2*ts)
    #s2.price = 2*ts

    f = Security.pandas_frame(products=[s0, s1, s2], item="price")
    pdt.assert_series_equal(f["A"], ts, check_names=False)
    pdt.assert_series_equal(f["B"], 2 * ts, check_names=False)

def test_reference(security):
    frame1 = Security.reference_frame(products=[security])
    frame2 = pd.DataFrame(index=["100"], columns=["Bloomberg Ticker", "fullname"], data=[["IBM US Equity", "Peter Maffay"]])
    pdt.assert_frame_equal(frame1, frame2, check_names=False)
