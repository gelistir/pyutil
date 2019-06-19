import pandas as pd
import pandas.util.testing as pdt
import pytest

#from pyutil.mongo.mongo import client, Collection
from pyutil.sql.interfaces.risk.security import Security
from test.config import read

t0 = pd.Timestamp("1978-11-15")
t1 = pd.Timestamp("1978-11-16")


#@pytest.fixture(scope="module")
#def collection():
#    db = client('test-mongo', 27017)['test-database']
#    c = Collection(collection=db.test_collection)
#    return c


@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)


class TestSecurity(object):
    def test_name(self):
        s = Security(name=100)
        assert s.name == "100"
        assert s.discriminator == "security"
        assert str(s) == "Security(100: None)"

        #s.upsert_price(test_portfolio().prices["A"])

        #a = s.to_json()
        #assert isinstance(a, dict)
        #assert a["name"] == "100"

        #pdt.assert_series_equal(a["Price"], s.price)

        #s.upsert_volatility(Currency(name="USD"), ts=pd.Series([30, 40, 50]))
        #s.upsert_volatility(Currency(name="CHF"), ts=pd.Series([10, 11, 12]))

        #pdt.assert_frame_equal(s.vola_frame, pd.DataFrame({key: item for (key, item) in s._vola.items()}))

        assert s.bloomberg_scaling == 1
        assert not s.bloomberg_ticker

    def test_write(self, ts):
        s = Security(name=100)
        s.write(kind="PRICE", data=ts)
        x = s.read(kind="PRICE", parse=True)
        pdt.assert_series_equal(x, ts, check_names=False)

    def test_write_volatility(self, ts):
        s = Security(name=100)
        s.write(kind="VOLATILITY", data=ts, currency="USD")
        x = s.read(parse=True, kind="VOLATILITY")
        pdt.assert_series_equal(x, ts, check_names=False)

    def test_vola_frame(self):
        # todo: write a test
        pass

        #assert False
        # send collection ...