from unittest import TestCase

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.exc import NoResultFound

from pyutil.sql.base import Base
from pyutil.sql.session import session_test, get_one_or_create, get_one_or_none, session, session_scope
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
            name = Column(String)

            def __init__(self, name):
                self.name = name

        with session_scope(server="test-postgresql", db="postgres", password="test", user="postgres", echo=True) as session:
            # drop all tables (even if there are none)
            Base.metadata.drop_all(session.bind)
            # create some tables
            Base.metadata.create_all(session.bind)

            session.add(User(name="Peter Maffay"))
            session.add(User(name="Hans Dampf"))

            u = session.query(User).filter_by(name="Peter Maffay").one()
            self.assertIsNotNone(u)

            with self.assertRaises(NoResultFound):
                session.query(User).filter_by(name="Wurst").one()
