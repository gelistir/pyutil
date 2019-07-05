import pytest

from pyutil.mongo.mongo import create_collection
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.risk.custodian import Custodian, Currency
from pyutil.sql.interfaces.risk.owner import Owner
import pandas.util.testing as pdt

from test.config import read

# point to a new mongo collection...
ProductInterface.__collection__ = create_collection()

@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)


class TestOwner(object):
    def test_custodian(self):
        o = Owner(name="Peter")
        o.custodian = Custodian(name="UBS")
        assert o.custodian == Custodian(name="UBS")

    def test_timeseries(self, ts):
        c = Custodian(name="UBS")
        c.write(data=ts)
        pdt.assert_series_equal(c.read(parse=True), ts)


class TestCurrency(object):
    def test_currency(self):
        o = Owner(name="Peter")
        o.currency = Currency(name="CHF")
        assert o.currency == Currency(name="CHF")

    def test_timeseries(self, ts):
        c = Currency(name="CHF")
        c.write(data=ts)
        pdt.assert_series_equal(c.read(parse=True), ts)

