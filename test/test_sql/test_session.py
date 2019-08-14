import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from pyutil.sql.session import get_one_or_create, get_one_or_none, session as s_session
from pyutil.testing.database import database
from test.test_sql.maffay import Maffay, Base


@pytest.fixture()
def session():
    db = database(base=Base)
    yield db.session
    db.session.close()


class TestSession(object):
    def test_get_one_or_create(self, session):
        # exists is False, as the user "B" does not exist yet
        x, exists = get_one_or_create(session, Maffay, name="B")
        assert not exists
        session.commit()

        # The user has been created before...
        y, exists = get_one_or_create(session, Maffay, name="B")
        assert exists
        assert x == y

    def test_get_one_or_none(self, session):
        assert not get_one_or_none(session, Maffay, name="C")

    def test_session_scope(self, session):
        session.add(Maffay(name="Peter Maffay"))
        session.commit()
        u = session.query(Maffay).filter_by(name="Peter Maffay").one()

        assert u.name == "Peter Maffay"

        with pytest.raises(NoResultFound):
            session.query(Maffay).filter_by(name="Wurst").one()

    def test_throw_error(self, session):
        with pytest.raises(IntegrityError):
            session.add(Maffay(name="Peter Maffay"))
            session.commit()
            # we are trying to add the user a second time! Verboten!
            session.add(Maffay(name="Peter Maffay"))
            session.commit()

    def test_scope(self):
        connection_str = "sqlite:///:memory:"
        with s_session(connection_str=connection_str, base=Base) as s:
            # add the user Hans Dampf
            s.add(Maffay(name="Hans Dampf"))

            ## find the user Hans Dampf
            assert s.query(Maffay).filter_by(name="Hans Dampf").one()
            # do not find the user Peter Maffay
            with pytest.raises(NoResultFound):
                s.query(Maffay).filter_by(name="Peter Maffay").one()

    def test_exception(self):
        with pytest.raises(IntegrityError):
            connection_str = "sqlite:///:memory:"
            with s_session(connection_str=connection_str, base=Base) as s:
                s.add(Maffay(name="Hans"))
                s.add(Maffay(name="Hans"))

