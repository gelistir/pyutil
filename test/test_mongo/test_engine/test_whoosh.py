from pyutil.mongo.engine.whoosh import Whoosh
from test.config import mongo


def test_whoosh(mongo):
    with mongo as m:
        w = Whoosh(title="A", content="B", path="C", group="D")
        w.save()

        x = Whoosh.frame()
        assert not x.empty



