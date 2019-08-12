import pytest

from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.strategy.reader import assets, symbolmap
from pyutil.testing.database import database


@pytest.fixture()
def session():
    db = database(base=Base)

    assert db.session.query(Symbol).count() == 0

    # add one symbol to database
    s = Symbol(name="Maffay", group=SymbolType.fixed_income)
    db.session.add(s)
    db.session.commit()

    yield db.session
    db.session.close()


def test_assets_1(session):
    a = assets(session=session, names=["Maffay"])
    assert len(a) == 1
    assert a[0] == session.query(Symbol).filter(Symbol.name == "Maffay").one()


def test_assets_2(session):
    a = assets(session=session)
    assert len(a) == 1
    assert a[0] == session.query(Symbol).filter(Symbol.name == "Maffay").one()


def test_symbolmap(session):
    a = symbolmap(session=session)
    assert a == {"Maffay": SymbolType.fixed_income.name}