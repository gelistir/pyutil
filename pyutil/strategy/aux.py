import abc
import logging
import multiprocessing
import os

from sqlalchemy.orm import sessionmaker

from pyutil.influx.client import Client
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.symbols.strategy import Strategy, module
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.session import get_one_or_create


class Runner(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, engine, logger=None):
        self.__engine = engine
        self._logger = logger or logging.getLogger(__name__)
        self.__jobs = []

    @property
    def _session(self):
        """ Create a fresh new session... """
        self.__engine.dispose()
        factory = sessionmaker(self.__engine)
        return factory()

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
        """ Portfolio described by the Configuration """


class StrategyRunner(Runner):
    def __init__(self, engine, logger=None):
        super().__init__(engine, logger=logger)

    def run(self):
        for s in self._session.query(Strategy).filter(Strategy.active == True).all():
            # what shall I give to the Process? The strategy object, the strategy_id, a session instance, the session_scope...
            job = multiprocessing.Process(target=self._target, kwargs={"strategy_id": s.id})
            job.name = s.name
            self.jobs.append(job)

        self._run_jobs()

    # this function is the target for mp.Process
    def _target(self, strategy_id):
        self._logger.debug("Pid {pid}".format(pid=os.getpid()))

        session = self._session

        ProductInterface.client = Client()

        symbols = {s.name: s for s in session.query(Symbol)}

        # extract the strategy
        strategy = session.query(Strategy).filter_by(id=strategy_id).one()

        self._logger.info("Run strategy {s}".format(s=strategy))

        configuration = strategy.configuration()
        strategy.upsert(portfolio=configuration.portfolio, symbols=symbols, days=5)

    def upsert_strategies(self, folder):
        # set all strategies to inactive!
        self._logger.info("Pid {pid}".format(pid=os.getpid()))

        session = self._session

        for strat in session.query(Strategy):
            strat.active = False

        # initialize all strategies with source code
        for file in os.listdir(folder):
            with open(os.path.join(folder, file), "r") as f:
                source = f.read()
                m = module(source=source)

                strat, exists = get_one_or_create(session=session, model=Strategy, name=m.name)
                strat.active = True
                strat.source = source

                self._logger.info("Strategy {s} active".format(s=strat))

        # commit all changes made
        session.commit()
