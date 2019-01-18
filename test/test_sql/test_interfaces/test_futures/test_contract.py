import pandas as pd
import pytest

from pyutil.sql.interfaces.futures.category import FuturesCategory
from pyutil.sql.interfaces.futures.contract import Contract
from pyutil.sql.interfaces.futures.exchange import Exchange
from pyutil.sql.interfaces.futures.future import Future


def future():
    # define an exchange
    e = Exchange(name="Chicago Mercantile Exchange", exch_code="CME")
    # define a category
    c = FuturesCategory(name="Equity Index")
    # define the future
    return Future(name="ES1 Index", internal="S&P 500 E-Mini Futures", quandl="CME/ES", exchange=e, category=c)


class TestContract(object):
    def test_contract(self):
        # Contract doesn't need a future here...
        c = Contract(figi="B3BB5", notice=pd.Timestamp("2010-01-01").date(), bloomberg_symbol="AAA", fut_month_yr="MAR 00")
        assert c.notice == pd.Timestamp("2010-01-01").date()
        assert c.bloomberg_symbol == "AAA"
        assert c.fut_month_yr == "MAR 00"
        assert c.alive(today=pd.Timestamp("2009-11-17").date())
        assert not c.alive(today=pd.Timestamp("2010-02-03").date())
        assert c.discriminator == "Contract"
        assert c.figi == "B3BB5"
        assert c.name == "B3BB5"
        assert c.month_xyz == "MAR"
        assert c.month_x == "H"
        assert c.year == 2000
        assert str(c) == "Contract(B3BB5)"


        c = Contract(figi="B3BB5", notice=pd.Timestamp("1960-01-01").date(), bloomberg_symbol="AAA", fut_month_yr="MAR 60")
        assert c.year == 1960

    def test_invariance(self):
        c = Contract(figi="B3BB5", notice=pd.Timestamp("2010-01-01").date(), bloomberg_symbol="AAA", fut_month_yr="MAR 00")

        with pytest.raises(AttributeError):
            c.figi = "NoNoNo"


