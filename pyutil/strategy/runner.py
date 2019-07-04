import logging
from functools import partial
import multiprocessing as mp

import pandas as pd

from pyutil.portfolio.portfolio import Portfolio

from pyutil.mongo.mongo import mongo_client
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol


def __strategy_update(strategy_id, connection_str, logger):
    from pyutil.sql.session import session

    def reader(session):
        return lambda name: session.query(Symbol).filter(Symbol.name == name).one().price

    # make a fresh mongo client
    ProductInterface._client = mongo_client()

    # do a read is enough...
    with session(connection_str=connection_str) as session:
        logger.debug(connection_str)
        logger.debug(strategy_id)
        # extract the strategy you need
        strategy = session.query(Strategy).filter_by(id=strategy_id).one()
        # self.logger.debug("Strategy {s}".format(s=strategy.name))

        # this could be none...
        strategy_portfolio = strategy.portfolio

        portfolio_new = strategy.configuration(reader=reader(session)).portfolio

        if strategy_portfolio is not None:
            # cut off the last few days
            portfolio_new = portfolio_new.truncate(before=strategy_portfolio.last - pd.DateOffset(days=10))

        strategy.portfolio = Portfolio.merge(new=portfolio_new, old=strategy_portfolio)
        return strategy.name, strategy.portfolio


def run(strategies, connection_str, logger=None):
    pool = mp.Pool(mp.cpu_count())
    logger = logger or logging.getLogger(__name__)
    __update = partial(__strategy_update, connection_str=connection_str, logger=logger)
    return {r[0]: r[1] for r in pool.map(__update, [x.id for x in strategies])}
