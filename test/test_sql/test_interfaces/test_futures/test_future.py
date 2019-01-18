import pandas as pd
import pandas.util.testing as pdt
import pytest

from pyutil.sql.interfaces.futures.category import FuturesCategory
from pyutil.sql.interfaces.futures.contract import Contract
from pyutil.sql.interfaces.futures.exchange import Exchange
from pyutil.sql.interfaces.futures.future import Future


@pytest.fixture(scope="module")
def future():
    # define an exchange
    e = Exchange(name="Chicago Mercantile Exchange")
    # define a category
    c = FuturesCategory(name="Equity Index")
    # define the future
    future = Future(name="ES1 Index", internal="S&P 500 E-Mini Futures", quandl="CME/ES", exchange=e, category=c)

    c1 = Contract(figi="BB1", notice=pd.Timestamp("2010-01-01").date(), fut_month_yr="JAN 10")
    c2 = Contract(figi="BB2", notice=pd.Timestamp("2009-03-01").date(), fut_month_yr="MAR 09")

    #future.contracts.append(c1)
    #future.contracts.append(c2)
    future.contracts.extend([c2, c1])

    return future


class TestFuture(object):
    def test_future(self, future):
        assert future.name == "ES1 Index"
        assert future.category.name == "Equity Index"
        assert future.exchange.name == "Chicago Mercantile Exchange"
        assert future.internal == "S&P 500 E-Mini Futures"
        assert future.quandl == "CME/ES"
        assert str(future) == "Future(ES1 Index)"

    def test_future_no_contracts(self):
        # define an exchange
        f = Future(name="ES1 Index", internal="S&P 500 E-Mini Futures", quandl="CME/ES")
        assert not f.max_notice
        assert f.contracts == []

    def test_future_with_contracts(self, future):
        assert future.contracts[0].notice < future.contracts[1].notice
        assert future.contracts[0].future == future
        assert future.contracts[1].future == future

        # You can not modify the underlying future of a contract!
        with pytest.raises(AttributeError):
            c = future.contracts[0]
            c.future = future

        assert future.max_notice == pd.Timestamp("2010-01-01").date()
        assert future.figis, ["BB2", "BB1"]
        assert future.contracts[0].quandl == "CME/ESH2009"
        assert future.contracts[0] < future.contracts[1]

    def test_rollmap(self):
        f = Future(name="AZ1 Comdty", internal="HaHa", quandl="CME/ES")

        # add the contracts
        c1 = Contract(notice=pd.Timestamp("2014-01-01").date(), figi="A1",
                      bloomberg_symbol="AZ14 Comdty", fut_month_yr="Jan 14")
        c2 = Contract(notice=pd.Timestamp("2015-01-01").date(), figi="A2",
                      bloomberg_symbol="AZ15 Comdty", fut_month_yr="Jan 15")
        c3 = Contract(notice=pd.Timestamp("2016-01-01").date(), figi="A3",
                      bloomberg_symbol="AZ16 Comdty", fut_month_yr="Jan 16")
        c4 = Contract(notice=pd.Timestamp("2017-01-01").date(), figi="A4",
                      bloomberg_symbol="AZ17 Comdty", fut_month_yr="Jan 17")

        # use an abritrary order here...
        f.contracts.extend([c4, c2, c3, c1])
        t0 = pd.Timestamp("2014-12-11")

        x = f.roll_builder(offset_days=5).trunc(before=t0)

        pdt.assert_series_equal(x, pd.Series(index=pd.DatetimeIndex([t0.date(), pd.Timestamp("2014-12-27").date(), pd.Timestamp("2015-12-27").date()]), data=[c2,c3,c4]))


        x = f.roll_builder(offset_days=5).trunc(before=pd.Timestamp("2013-12-27"))

        pdt.assert_series_equal(x, pd.Series(index=pd.DatetimeIndex([pd.Timestamp("2013-12-27").date(), pd.Timestamp("2014-12-27").date(), pd.Timestamp("2015-12-27").date()]), data=[c2, c3, c4]))
