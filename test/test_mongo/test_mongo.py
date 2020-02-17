from pyutil.mongo.engine.currency import Currency
from pyutil.mongo.mongo import Mongo


def test_stack():
    with Mongo(db="test", host="mongomock://localhost") as m:
        Currency(name="USD").save()
        assert isinstance(m, Mongo)


