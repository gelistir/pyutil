import logging
import multiprocessing
import os
from abc import ABCMeta, ABC, abstractmethod
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import functools
import traceback
import sys


class XSession(object):
    def __init__(self, connection_str=None):
        self._connection_str = connection_str
        #self._logger = logger or logging.getLogger(__name__)
        #self.__jobs = []

    def engine(self, echo=False):
        """ Create a fresh new session... """
        return create_engine(self._connection_str, echo=echo)

    def connection(self, echo=False):
        return self.engine(echo=echo).connect()

    def _session(self, echo=False):
        return Session(bind=self.connection(echo=echo))

    @contextmanager
    def session(self, echo=False):
        """Provide a transactional scope around a series of operations."""
        try:
            s = self._session(echo=echo)
            yield s
            s.commit()
        except Exception as e:
            s.rollback()
            raise e
        finally:
            s.close()


def get_traceback(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as ex:
            ret = "\nException caught:"
            ret += "\n" + '-' * 60
            ret += "\n" + traceback.format_exc()
            ret += "\n" + '-' * 60
            print(sys.stderr, ret)
            sys.stderr.flush()
            raise ex

    return wrapper

class Worker(ABC, multiprocessing.Process):

    def __init__(self, name, logger=None):
        super().__init__()
        self.name = name
        self.logger = logger or logging.getLogger(__name__)

    @abstractmethod
    @get_traceback
    def run(self):
        """ overwrite """

    #def _session(self):


class Runner(object):
    def __init__(self, connection_str=None, logger=None):
        self._connection_str = connection_str
        self._logger = logger or logging.getLogger(__name__)
        self.__jobs = []

    def engine(self, echo=False):
        """ Create a fresh new session... """
        return create_engine(self._connection_str, echo=echo)

    def connection(self, echo=False):
        return self.engine(echo=echo).connect()

    def _session(self, echo=False):
        return Session(bind=self.connection(echo=echo))

    @contextmanager
    def session(self, echo=False):
        """Provide a transactional scope around a series of operations."""
        try:
            s = self._session(echo=echo)
            yield s
            s.commit()
        except Exception as e:
            s.rollback()
            raise e
        finally:
            s.close()

    def run_jobs(self):
        self._logger.debug("PID main {pid}".format(pid=os.getpid()))

        for job in self.jobs:
            # all jobs get the trigge
            self._logger.info("Job {j}".format(j=job.name))
            job.start()

        for job in self.jobs:
            self._logger.info("Wait for job {j}".format(j=job.name))
            job.join()
            assert job.exitcode == 0, "Problem with job {j}".format(j = job)
            self._logger.info("Job {j} done".format(j=job.name))


    @property
    def jobs(self):
        return self.__jobs

    def append_job(self, job):
        self.jobs.append(job)
        return job

