class Runner(object):
    def __init__(self):
        self.jobs = []

    def run_jobs(self):
        for job in self.jobs:
            # all jobs start
            job.start()

        for job in self.jobs:
            # wait for the job
            job.join()
            assert job.exitcode == 0, "Problem with job {j}".format(j=job)
