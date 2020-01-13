from pyutil.mongo.engine.custodian import Custodian


def test_custodian():
    c1 = Custodian(name="A")
    assert c1.name == "A"
