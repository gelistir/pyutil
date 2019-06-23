import logging
import multiprocessing
import pandas as pd

from pyutil.mongo.mongo import mongo_client
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.portfolio.portfolio import Portfolio

from pyutil.runner import Runner

def f(configuration):
    return configuration.portfolio


def run(session, logger=None):
    logger = logger or logging.getLogger(__name__)
    runner = __StrategyRunner2(logger=logger)

    for strategy in session.query(Strategy).all():   # filter...
        assets = strategy.assets

        #extract frame

        #make reader function

        #init configuration

        #runner.append(configuration)

        # configurations...

    # configurations is a dict of configuration objects


    # run all the jobs
    # runner.run_jobs()


class __StrategyRunner2(Runner):
    def __init__(self, logger=None):
        super().__init__(logger=logger)

    def append(self, strategy_id):
        job = multiprocessing.Process(target=self.target, kwargs={"configuration": configuration})
        self.jobs.append(job)

    def target(self, configuration):
        # this is where all the heavy work is done
        return configuration.portfolio


    # this function is the target for mp.Process
    #def target(self, strategy_id):
    #    from pyutil.sql.session import session

    #    ProductInterface._client = mongo_client()

        # do a read is enough...
    #    with session(connection_str=self.__connection_str) as session:
    #        # extract the strategy you need
    #        strategy = session.query(Strategy).filter_by(id=strategy_id).one()
    #        self.logger.debug("Strategy {s}".format(s=strategy.name))

    #        # this could be none...
    #        strategy_portfolio = strategy.portfolio

    #        portfolio_new = strategy.configuration(reader=self.reader(session)).portfolio

     #       if strategy_portfolio is not None:
     #           # cut off the last few days
     #           portfolio_new = portfolio_new.truncate(before=strategy_portfolio.last - pd.DateOffset(days=10))

     #       strategy.portfolio = Portfolio.merge(new=portfolio_new, old=strategy_portfolio)
