# from unittest import TestCase
#
# from pyutil.sql.base import Base
# from pyutil.sql.db_symbols import DatabaseSymbols
# from pyutil.sql.runner import Runner
# from pyutil.sql.session import session_test
#
#
# class TestRunner(TestCase):
#     def test_runner(self):
#         session = session_test(Base.metadata)
#         r = Runner(DatabaseSymbols, session=session)
#         self.assertEqual(r.session, session)
#
#         self.assertIsNotNone(r.database)
#         self.assertIsNotNone(r.logger)
