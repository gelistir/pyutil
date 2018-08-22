from unittest import TestCase

from pyutil.sql.runner import Runner, get_traceback, Worker
from pyutil.sql.session import postgresql_db_test, str_postgres
from test.test_sql.user import User, Base


class WorkerImpl1(Worker):
    def __init__(self, name, x):
        super().__init__(name=name)
        self.x = x

    @get_traceback
    def run(self):
        assert False

class WorkerImpl2(Worker):
    def __init__(self, name, x):
        super().__init__(name=name)
        self.x = x

    @get_traceback
    def run(self):
        assert True


class TestRunner(TestCase):
    @classmethod
    def setUpClass(cls):
        # create an empty database
        postgresql_db_test(base=Base, name="maffay")

    def test_Runner_Faulty(self):
        runner = Runner()

        # worker will raise an Exception!
        with self.assertRaises(AssertionError):
            runner.append_job(WorkerImpl1(name="Peter", x=10))
            runner.run_jobs()

    def test_Runner_Correct(self):
        runner = Runner()

        # worker will not raise an Exception!
        runner.append_job(WorkerImpl2(name="Peter", x=10))
        runner.run_jobs()

    def test_Runner_empty(self):
        runner = Runner()

        self.assertListEqual(runner.jobs, [])
        # this won't do any harm
        runner.run_jobs()

    def test_session(self):
        runner = Runner(connection_str=str_postgres(db="maffay"))
        with runner.session() as session:
            self.assertIsNotNone(session)
            session.add(User(name="Hans"))
            session.add(User(name="Dampf"))

        # it will now commit both users as soon as we exit the with statement
        # this will now create a second fresh session from scratch!
        with runner.session() as session:
            x = session.query(User).filter_by(name="Hans").one()
            self.assertIsNotNone(x)
            self.assertIsInstance(x, User)
