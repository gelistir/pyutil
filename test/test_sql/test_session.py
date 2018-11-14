from unittest import TestCase

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from pyutil.sql.session import get_one_or_create, get_one_or_none, postgresql_db_test, session
from test.test_sql.user import User, Base


class TestSession(TestCase):
    def test_get_one_or_create(self):
        session, connection_str = postgresql_db_test(base=Base)

        # exists is False, as the user "B" does not exist yet
        x, exists = get_one_or_create(session, User, name="B")
        self.assertFalse(exists)

        # The user has been created before...
        y, exists = get_one_or_create(session, User, name="B")
        self.assertTrue(exists)

        self.assertEqual(x, y)

    def test_get_one_or_none(self):
        session, _ = postgresql_db_test(base=Base)
        self.assertIsNone(get_one_or_none(session, User, name="C"))

    def test_session_scope(self):
        session, _ = postgresql_db_test(base=Base)

        session.add(User(name="Peter Maffay"))
        u = session.query(User).filter_by(name="Peter Maffay").one()

        self.assertEqual(u.name, "Peter Maffay")

        with self.assertRaises(NoResultFound):
            session.query(User).filter_by(name="Wurst").one()

    def test_throw_error(self):
        session, _ = postgresql_db_test(base=Base)
        with self.assertRaises(IntegrityError):
            session.add(User(name="Peter Maffay"))
            # we are trying to add the user a second time! Verboten!
            session.add(User(name="Peter Maffay"))
            session.commit()

    def test_session(self):
        _, connection_str = postgresql_db_test(base=Base)
        with session(connection_str=connection_str) as s:
            s.add(User(name="Peter Maffay 2"))



