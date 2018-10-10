import multiprocessing
from unittest import TestCase

from pyutil.runner import Runner


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


class TestRunner(TestCase):
    def test_Runner_Faulty(self):
        runner = Runner()

        # worker will raise an Exception!
        with self.assertRaises(AssertionError):
            runner.jobs.append(WorkerImpl1(name="Peter", x=10))
            runner.run_jobs()

    def test_Runner_Correct(self):
        runner = Runner()

        # worker will not raise an Exception!
        runner.jobs.append(WorkerImpl2(name="Peter", x=10))
        runner.run_jobs()

    def test_Runner_empty(self):
        runner = Runner()

        self.assertListEqual(runner.jobs, [])
        # this won't do any harm
        runner.run_jobs()

