import logging
import multiprocessing
import os

from pyutil.sql.session import session as session_cm


class Runner(object):
    def __init__(self, sql=None, logger=None):
        self._sql = sql

        self._logger = logger or logging.getLogger(__name__)
        self.__jobs = []

    def session(self, echo=False):
        return session_cm(connection_str=self._sql, echo=echo)

    def run_jobs(self):
        self._logger.debug("PID main {pid}".format(pid=os.getpid()))

        for job in self.jobs:
            # all jobs get the trigge
            self._logger.info("Job {j}".format(j=job.name))
            job.start()

        for job in self.jobs:
            self._logger.info("Wait for job {j}".format(j=job.name))
            job.join()
            assert job.exitcode == 0, "Problem with job {j}".format(j=job)
            self._logger.info("Job {j} done".format(j=job.name))

    @property
    def jobs(self):
        return self.__jobs

    def _append_job(self, job):
        self.jobs.append(job)
        return job

    def iterate_objects(self, object_cls, target):
        with self.session() as session:
            for obj in session.query(object_cls).all():
                self._logger.debug("Object {s}".format(s=obj))
                j = multiprocessing.Process(target=target, args=(obj.id, ))
                j.name = obj.name
                self._append_job(job=j)

            self.run_jobs()

