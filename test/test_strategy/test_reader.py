import pytest
from sqlalchemy.orm.exc import NoResultFound

from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType
from pyutil.strategy.reader import reader, assets
from pyutil.testing.aux import postgresql_db_test
from test.config import read

import pandas.util.testing as pdt


@pytest.fixture()
def session():
    db = postgresql_db_test(Base)

    session = db.session

    s = Symbol(name="Maffay", group=SymbolType.fixed_income)
    session.add(s)
    session.commit()

    return session

@pytest.fixture()
def price():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True)

def test_reader(session, price):
    s = session.query(Symbol).filter(Symbol.name == "Maffay").one()
    s.upsert_price(ts=price)

    f = reader(session)
    pdt.assert_series_equal(f(name="Maffay"), price)

    with pytest.raises(NoResultFound):
        f(name="unknown")

def test_assets(session):
    a = assets(session=session, names=["Maffay"])
    assert len(a) == 1
    assert a[0] == session.query(Symbol).filter(Symbol.name == "Maffay").one()

