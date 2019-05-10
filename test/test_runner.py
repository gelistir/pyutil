import multiprocessing

import pytest

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


class TestRunner(object):
    def test_Runner_Faulty(self):
        runner = Runner()

        # worker will raise an Exception!
        with pytest.raises(AssertionError):
            runner.jobs.append(WorkerImpl1(name="Peter", x=10))
            runner.run_jobs()

    def test_Runner_Correct(self):
        runner = Runner()

        # worker will not raise an Exception!
        runner.jobs.append(WorkerImpl2(name="Peter", x=10))
        runner.run_jobs()

        # check the logger
        assert runner.logger

    def test_Runner_empty(self):
        runner = Runner()

        assert runner.jobs == []
        # this won't do any harm
        runner.run_jobs()

