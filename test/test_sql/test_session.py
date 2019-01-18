import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from pyutil.sql.session import get_one_or_create, get_one_or_none
from pyutil.test.aux import postgresql_db_test
from test.test_sql.user import User, Base

@pytest.fixture()
def session():
    db = postgresql_db_test(Base)
    return db.session


class TestSession(object):
    def test_get_one_or_create(self, session):
        # exists is False, as the user "B" does not exist yet
        x, exists = get_one_or_create(session, User, name="B")
        assert not exists
        session.commit()

        # The user has been created before...
        y, exists = get_one_or_create(session, User, name="B")
        assert exists
        assert x == y

    def test_get_one_or_none(self, session):
        assert not get_one_or_none(session, User, name="C")

    def test_session_scope(self, session):
        session.add(User(name="Peter Maffay"))
        session.commit()
        u = session.query(User).filter_by(name="Peter Maffay").one()

        assert u.name == "Peter Maffay"

        with pytest.raises(NoResultFound):
            session.query(User).filter_by(name="Wurst").one()

    def test_throw_error(self, session):
        with pytest.raises(IntegrityError):
            session.add(User(name="Peter Maffay"))
            session.commit()
            # we are trying to add the user a second time! Verboten!
            session.add(User(name="Peter Maffay"))
            session.commit()
