from pyutil.mongo.engine.whoosh import Whoosh
from test.config import *


def test_whoosh():
    Whoosh(title="A", content="B", path="C", group="D").save()
    x = Whoosh.frame()
    assert not x.empty



