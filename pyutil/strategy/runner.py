import logging
import multiprocessing

from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.runner import Runner


def run(strategies, connection_str, logger=None):
    logger = logger or logging.getLogger(__name__)
    runner = __StrategyRunner(connection_str=connection_str, logger=logger)

    for strategy in strategies:
        runner.append(strategy.id)

    # run all the jobs
    runner.run_jobs()


class __StrategyRunner(Runner):
    def __init__(self, connection_str, logger=None):
        super().__init__(logger=logger)
        self.__connection_str = connection_str

    # this returns a function f(name) = price of Symbol with name==name
    def reader(self, session):
        return lambda name: session.query(Symbol).filter(Symbol.name == name).one().price

    def append(self, strategy_id):
        job = multiprocessing.Process(target=self.target, kwargs={"strategy_id": strategy_id})
        self.jobs.append(job)

    # this function is the target for mp.Process
    def target(self, strategy_id):
        from pyutil.sql.session import session

        # do a read is enough...
        with session(connection_str=self.__connection_str) as session:

            # get all symbols into a big dictionary
            symbols = {s.name: s for s in session.query(Symbol)}

            # extract the strategy you need
            strategy = session.query(Strategy).filter_by(id=strategy_id).one()

            self.logger.info("Last timestamp {t} for {s}".format(t=strategy.last, s=strategy))


            configuration = strategy.configuration(reader=self.reader(session))
            strategy.upsert(portfolio=configuration.portfolio, symbols=symbols, days=5)
