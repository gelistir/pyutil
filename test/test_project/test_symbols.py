import pandas as pd
import pytest

from pyutil.mongo.engine.symbol import Group, Symbol
from test.config import mongo


@pytest.fixture()
def ts():
    return pd.Series(data=[2, 4, 6])


@pytest.fixture()
def symbol(mongo, ts):
    with mongo as m:
        g = Group(name="Alternatives")
        g.save()
        s = Symbol(name="A", internal="AAA", group=g)

        s.reference["XXX"] = 10
        s.price = ts
        s.save()
        return s


@pytest.fixture()
def symbols(mongo):
    with mongo as m:
        a = Group(name="Alternatives")
        a.save()

        c = Group(name="Currency")
        c.save()

        s1 = Symbol(name="A", group=a, internal="AAA")
        s2 = Symbol(name="B", group=c, internal="BBB")
        return [s1, s2]


class TestSymbols(object):
    def test_symbol(self, mongo, symbols):
        with mongo as m:
            # get all symbols from database
            #for symbol in symbols:
            #    symbol.save()

            assert Symbol.symbolmap(symbols) == {"A": "Alternatives", "B": "Currency"}

    def test_meta(self, symbol):
        assert symbol.internal == "AAA"
        assert symbol.group.name == "Alternatives"

    def test_reference_frame(self, symbol):
        frame = Symbol.reference_frame(products=[symbol])
        assert frame.index.name == "symbol"
        assert frame["XXX"]["A"] == 10
        assert frame["Sector"]["A"] == "Alternatives"
        assert frame["Internal"]["A"] == "AAA"

    # def test_delete(self, session):
    #    Symbol.delete(session=session, name="A")
    #    # make sure the symbol no longer exists
    #    with pytest.raises(NoResultFound):
    #        session.query(Symbol).filter(Symbol.name=="A").one()
