import logging
import os


class Runner(object):
    def __init__(self, logger=None):
        self._logger = logger or logging.getLogger(__name__)
        self.jobs = []

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
