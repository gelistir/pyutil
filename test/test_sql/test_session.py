
from unittest import TestCase

from sqlalchemy import Column, Integer, String
from psycopg2 import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.exc import NoResultFound

from pyutil.sql.base import Base
from pyutil.sql.session import session_test, get_one_or_create, get_one_or_none, session, postgresql_db_test, session_scope
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.interfaces.risk.currency import Currency


class TestSession(TestCase):
    def test_get_one_or_create(self):
        session = session_test(meta=Base.metadata, echo=True)

        x, exists = get_one_or_create(session, Symbol, name="B")
        self.assertFalse(exists)

        y, exists = get_one_or_create(session, Symbol, name="B")
        self.assertTrue(exists)

        self.assertEqual(x, y)

    def test_get_one_or_none(self):
        session = session_test(meta=Base.metadata, echo=True)
        self.assertIsNone(get_one_or_none(session, Symbol, name="C"))

    def test_get_one_or_create_currency(self):
        session = session_test(meta=Base.metadata, echo=False)

        # this works because name is a hybrid property of Currency!
        x, exists = get_one_or_create(session, Currency, name="CHF")
        self.assertFalse(exists)

        y, exists = get_one_or_create(session, Currency, name="CHF")
        self.assertTrue(exists)

    def test_postgresql_session(self):
        p_session = session(server="test-postgresql", db="postgres", password="test", user="postgres", echo=True)
        self.assertIsNotNone(p_session)

    def test_session_scope(self):
        Base = declarative_base()

        class User(Base):
            __tablename__ = "user"
            id = Column(Integer, primary_key=True)
            name = Column(String, unique=True)

            def __init__(self, name):
                self.name = name

        s = postgresql_db_test(base=Base)
        with session_scope(session=s) as session:
            session.add(User(name="Peter Maffay"))
            u = session.query(User).filter_by(name="Peter Maffay").one()
            self.assertIsNotNone(u)

            with self.assertRaises(NoResultFound):
                session.query(User).filter_by(name="Wurst").one()

        s = postgresql_db_test(base=Base)
        with session_scope(session=s) as session:
            with self.assertRaises(IntegrityError):
                session.add(User(name="Peter Maffay"))
                session.add(User(name="Peter Maffay"))
