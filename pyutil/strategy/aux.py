import abc
import logging
import os
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session


class Runner(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, connection_str, logger=None):
        self._connection_str = connection_str
        self._logger = logger or logging.getLogger(__name__)
        self.__jobs = []

    @property
    def _engine(self):
        """ Create a fresh new session... """
        #self._engine.dispose()
        return create_engine(self._connection_str, echo=False)
        #conn = engine.connect()
        #return engine
        #factory = sessionmaker(self.__engine)
        #return factory()

    @property
    def _connection(self):
        return self._engine.connect()

    @property
    def _session(self):
        return Session(bind=self._connection)

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        try:
            s = self._session
            yield s
            s.commit()
        except Exception as e:
            s.rollback()
            raise e
        finally:
            s.close()

    def _run_jobs(self):
        self._logger.debug("PID main {pid}".format(pid=os.getpid()))

        for job in self.jobs:
            # all jobs get the trigge
            self._logger.info("Job {j}".format(j=job.name))
            job.start()

        for job in self.jobs:
            self._logger.info("Wait for job {j}".format(j=job.name))
            job.join()
            self._logger.info("Job {j} done".format(j=job.name))

    @property
    def jobs(self):
        return self.__jobs

    @abc.abstractmethod
    def run(self):
        """ Described in the child class """

