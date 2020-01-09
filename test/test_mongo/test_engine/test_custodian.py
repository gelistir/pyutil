import pytest
from mongoengine import NotUniqueError

from pyutil.mongo.engine.custodian import Custodian, Currency
from test.config import mongo_client


def test_custodian(mongo_client):
    c1 = Custodian(name="A")
    c2 = Custodian(name="A")

    c1.save()
    with pytest.raises(NotUniqueError):
        c2.save()


def test_currency():
    c1 = Currency(name="USD")
    assert c1.name == "USD"
