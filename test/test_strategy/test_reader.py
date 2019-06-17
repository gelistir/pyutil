import pytest

from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.strategy.reader import assets
from pyutil.testing.database import database

from pyutil.sql.base import Base

@pytest.fixture()
def session():
    db = database(base=Base)

    s = Symbol(name="Maffay", group=SymbolType.fixed_income)
    db.session.add(s)
    db.session.commit()

    yield db.session
    db.session.close()


def test_assets(session):
    a = assets(session=session, names=["Maffay"])
    assert len(a) == 1
    assert a[0] == session.query(Symbol).filter(Symbol.name == "Maffay").one()

