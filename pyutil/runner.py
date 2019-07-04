import logging
import multiprocessing
from abc import abstractmethod


class Runner(object):
    def __init__(self, logger=None):
        self.__logger = logger or logging.getLogger(__name__)
        self.jobs = []

    @property
    def logger(self):
        return self.__logger

    def run_jobs(self):
        for job in self.jobs:
            # all jobs start
            job.start()

        for job in self.jobs:
            # wait for the job
            job.join()
            assert job.exitcode == 0, "Problem with job {j}".format(j=job)

    def append(self, **kwargs):
        # kwargs is a dictionary!
        job = multiprocessing.Process(target=self.target, kwargs=kwargs)
        self.jobs.append(job)
        return self

    @abstractmethod
    def target(self, **kwargs):
        """ nothing going on here"""
