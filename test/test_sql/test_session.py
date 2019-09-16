import pandas as pd
import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.session import get_one_or_create, get_one_or_none, session as s_session
from pyutil.testing.database import database


@pytest.fixture()
def session():
    db = database(base=Base)
    yield db.session
    db.session.close()


class TestSession(object):
    def test_get_one_or_create(self, session):
        # exists is False, as the user "B" does not exist yet
        x, exists = get_one_or_create(session, Symbol, name="B")
        assert not exists
        session.commit()

        # The user has been created before...
        y, exists = get_one_or_create(session, Symbol, name="B")
        assert exists
        assert x == y

    def test_get_one_or_none(self, session):
        assert not get_one_or_none(session, Symbol, name="C")

    def test_session_scope(self, session):
        session.add(Symbol(name="Peter Maffay"))
        session.commit()
        u = session.query(Symbol).filter_by(name="Peter Maffay").one()

        assert u.name == "Peter Maffay"

        with pytest.raises(NoResultFound):
            session.query(Symbol).filter_by(name="Wurst").one()

    def test_throw_error(self, session):
        with pytest.raises(IntegrityError):
            session.add(Symbol(name="Peter Maffay"))
            session.commit()
            # we are trying to add the user a second time! Verboten!
            session.add(Symbol(name="Peter Maffay"))
            session.commit()

    def test_scope(self):
        connection_str = "sqlite:///:memory:"
        with s_session(connection_str=connection_str, base=Base) as s:
            # add the user Hans Dampf
            s.add(Symbol(name="Hans Dampf"))

            ## find the user Hans Dampf
            assert s.query(Symbol).filter_by(name="Hans Dampf").one()
            # do not find the user Peter Maffay
            with pytest.raises(NoResultFound):
                s.query(Symbol).filter_by(name="Peter Maffay").one()

    def test_exception(self):
        with pytest.raises(IntegrityError):
            connection_str = "sqlite:///:memory:"
            with s_session(connection_str=connection_str, base=Base) as s:
                s.add(Symbol(name="Hans"))
                s.add(Symbol(name="Hans"))

    def test_delete(self, session):
        # exists is False, as the user "B" does not exist yet
        x, exists = get_one_or_create(session, Symbol, name="B")
        assert not exists
        session.commit()

        x.reference["A"] = "B"
        x.series.write(data=pd.Series(data=[1,2]), key="PX_LAST")

        Symbol.delete(session=session, name="B")

        with pytest.raises(NoResultFound):
            session.query(Symbol).filter(Symbol.name=="B").one()

        Symbol.delete(session=session, name="XXZJYJLKA")
