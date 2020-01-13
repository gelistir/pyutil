from pyutil.mongo.engine.currency import Currency


def test_currency():
    c1 = Currency(name="USD")
    assert c1.name == "USD"
