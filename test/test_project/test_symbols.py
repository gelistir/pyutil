import pandas as pd
import pytest

from pyutil.mongo.engine.symbol import Group, Symbol
from test.config import mongo_client

@pytest.fixture()
def ts():
    return pd.Series(data=[2, 4, 6])


@pytest.fixture()
def symbol(mongo_client, ts):
    g = Group(name = "Alternatives")
    g.save()
    s = Symbol(name="A", internal="AAA", group=g)

    s.reference["XXX"] = 10
    s.price = ts
    return s


@pytest.fixture()
def symbols(mongo_client):
    a = Group(name="Alternatives")
    a.save()

    c = Group(name="Currency")
    c.save()

    s1 = Symbol(name="A", group=a, internal="AAA")
    s2 = Symbol(name="B", group=c, internal="BBB")
    return [s1, s2]



class TestSymbols(object):
    def test_symbol(self, mongo_client, symbols):
        # get all symbols from database
        for symbol in symbols:
            symbol.save()

        a = Symbol.products()
        assert len(a) == 2
        assert set(symbols) == set(a)
        assert Symbol.symbolmap(symbols) == {"A": "Alternatives", "B": "Currency"}

    def test_symbols(self, mongo_client, symbols):
        for symbol in symbols:
            symbol.save()

        # get only one symbol from database
        a = Symbol.products(names=["A"])
        #print([s for s in Symbol.objects])
        assert len(a) == 1
        assert a[0] == symbols[0]

    def test_meta(self, symbol):
        assert symbol.internal == "AAA"
        assert symbol.group.name == "Alternatives"

    def test_reference_frame(self, symbol):
        frame = Symbol.reference_frame(products=[symbol], f=lambda x: x.name)
        assert frame.index.name == "symbol"
        assert frame["XXX"]["A"] == 10
        assert frame["Sector"]["A"] == "Alternatives"
        assert frame["Internal"]["A"] == "AAA"

    #def test_delete(self, session):
    #    Symbol.delete(session=session, name="A")
    #    # make sure the symbol no longer exists
    #    with pytest.raises(NoResultFound):
    #        session.query(Symbol).filter(Symbol.name=="A").one()

