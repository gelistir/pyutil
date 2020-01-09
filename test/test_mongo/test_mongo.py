from pyutil.mongo.engine.custodian import Currency
from pyutil.mongo.mongo import Mongo


def test_stack():
    with Mongo(db="test", host="mongomock://localhost") as m:
        c = Currency(name="USD")
        c.save()
        assert isinstance(m, Mongo)


