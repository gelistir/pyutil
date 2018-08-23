import multiprocessing
from multiprocessing import Manager
from unittest import TestCase

from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.runner import Runner
from pyutil.sql.session import postgresql_db_test

class WorkerImpl1(multiprocessing.Process):
    def __init__(self, name, x):
        super().__init__(name=name)
        self.x = x

    def run(self):
        assert False


class WorkerImpl2(multiprocessing.Process):
    def __init__(self, name, x):
        super().__init__(name=name)
        self.x = x

    def run(self):
        assert True


class RunnerImpl(Runner):
    def __init__(self, connection_str):
        super().__init__(connection_str)
        manager = Manager()
        self.__dict = manager.dict()

    def target(self, strategy_id):
        with self.session() as session:
            s = session.query(Strategy).filter_by(id=strategy_id).one()
            self.__dict[s.name] = s.name
            print(s)

    @property
    def dict(self):
        return self.__dict


class TestRunnerSession(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session, cls.connection_str = postgresql_db_test(base=Base)
        cls.session.add_all([Strategy(name="Peter"), Strategy(name="Maffay")])
        cls.session.commit()

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def test_iterate_strategies(self):
        runner = RunnerImpl(connection_str=self.connection_str)
        runner.iterate_objects(object_cls=Strategy, target=runner.target)
        print(runner.dict)
        self.assertDictEqual(dict(runner.dict),  {'Peter': 'Peter', 'Maffay': 'Maffay'})


class TestRunner(TestCase):
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

    #def test_session(self):
        #runner = Runner(connection_str=self.connection_str)
        #with runner.session() as session:
        #    self.assertIsNotNone(session)
        #    session.add(User(name="Hans"))
        #    session.add(User(name="Dampf"))

        # it will now commit both users as soon as we exit the with statement
        # this will now create a second fresh session from scratch!
        #with runner.session() as session:
        #    x = session.query(User).filter_by(name="Hans").one()
        #    self.assertIsNotNone(x)
        #    self.assertIsInstance(x, User)
