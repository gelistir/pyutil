import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.interfaces.risk.custodian import Currency
from pyutil.sql.interfaces.risk.security import Security
from test.config import test_portfolio

t0 = pd.Timestamp("1978-11-15")
t1 = pd.Timestamp("1978-11-16")


class TestSecurity(object):
    def test_name(self):
        s = Security(name=100)
        assert s.name == "100"

        #self.assertEqual(s.name, "100")
        assert not s._price
        assert s.discriminator == "security"
        assert str(s) == "Security(100: None)"

        s.upsert_price(test_portfolio().prices["A"])

        a = s.to_json()
        assert isinstance(a, dict)
        assert a["name"] == "100"

        pdt.assert_series_equal(a["Price"], s.price)

        s.upsert_volatility(Currency(name="USD"), ts=pd.Series([30, 40, 50]))
        s.upsert_volatility(Currency(name="CHF"), ts=pd.Series([10, 11, 12]))

        pdt.assert_frame_equal(s.vola_frame, pd.DataFrame({key: item for (key, item) in s._vola.items()}))

        assert s.bloomberg_scaling == 1
        assert not s.bloomberg_ticker
