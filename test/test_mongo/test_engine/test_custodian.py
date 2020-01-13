from pyutil.mongo.engine.currency import Currency
from pyutil.mongo.engine.custodian import Custodian


def test_custodian():
    c1 = Custodian(name="A")
    assert c1.name == "A"


def test_currency():
    c1 = Currency(name="USD")
    assert c1.name == "USD"
