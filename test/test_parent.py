from unittest import TestCase

from pyutil.parent import Production
from pyutil.sql.db import define_database


class TestDecorator(TestCase):

    def test_without_db(self):
        with Production() as p:
            self.assertIsNotNone(p.logger)
            self.assertIsNone(p.database)

    def test_with_db(self):
        db = define_database(provider='sqlite', filename=":memory:")
        with Production(db=db) as p:
            self.assertIsNotNone(p.database)
