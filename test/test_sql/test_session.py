from unittest import TestCase

from sqlalchemy import Column, String
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.exc import NoResultFound

from pyutil.sql.session import session_test, get_one_or_create, get_one_or_none, session, postgresql_db_test, \
    session_scope

Base = declarative_base()

class User(Base):
    __tablename__ = "user"
    name = Column(String, primary_key=True, unique=True)

    def __init__(self, name):
        self.name = name


class TestSession(TestCase):
    def test_get_one_or_create(self):
        session = session_test(meta=Base.metadata, echo=True)

        # exists is False, as the user "B" does not exist yet
        x, exists = get_one_or_create(session, User, name="B")
        self.assertFalse(exists)

        # The user has been created before...
        y, exists = get_one_or_create(session, User, name="B")
        self.assertTrue(exists)

        self.assertEqual(x, y)

    def test_get_one_or_none(self):
        session = session_test(meta=Base.metadata, echo=True)
        self.assertIsNone(get_one_or_none(session, User, name="C"))

    def test_postgresql_session(self):
        p_session = session(server="test-postgresql", db="postgres", password="test", user="postgres", echo=True)
        self.assertIsNotNone(p_session)

    def test_session_scope(self):
        s = postgresql_db_test(base=Base)
        with session_scope(session=s) as session:
            session.add(User(name="Peter Maffay"))
            u = session.query(User).filter_by(name="Peter Maffay").one()
            self.assertIsNotNone(u)

            with self.assertRaises(NoResultFound):
                session.query(User).filter_by(name="Wurst").one()

    def test_throw_error(self):
        s = postgresql_db_test(base=Base)
        with self.assertRaises(IntegrityError):
            with session_scope(session=s) as session:
                session.add(User(name="Peter Maffay"))
                session.add(User(name="Peter Maffay"))
