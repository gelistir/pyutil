import logging
from functools import partial
import multiprocessing as mp

import pandas as pd

from pyutil.portfolio.portfolio import Portfolio

from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol


def _strategy_update(strategy_id, connection_str, logger, n):
    from pyutil.sql.session import session

    def reader(session):
        return lambda name: session.query(Symbol).filter(Symbol.name == name).one().series["PX_LAST"]

    # do a read is enough...
    with session(connection_str=connection_str) as session:
        # extract the strategy you need

        # make fresh mongo clients
        Strategy.refresh_mongo()
        Symbol.refresh_mongo()


#        Strategy._client = mongo_client()
#        Symbol._client = mongo_client()

        strategy = session.query(Strategy).filter_by(id=strategy_id).one()
        last = strategy.last_valid_index

        logger.debug(strategy.name)
        logger.debug(last)

        portfolio_new = strategy.configuration(reader=reader(session)).portfolio

        if last is not None:
            # use only the very last few days...
            portfolio_new = portfolio_new.truncate(before=last - pd.DateOffset(days=n))
            strategy.portfolio = Portfolio.merge(new=portfolio_new, old=strategy.portfolio)
        else:
            strategy.portfolio = portfolio_new

        return strategy.name, strategy.portfolio


def run(strategy_ids, connection_str, logger=None, n=10):
    pool = mp.Pool(mp.cpu_count())
    logger = logger or logging.getLogger(__name__)
    __update = partial(_strategy_update, connection_str=connection_str, logger=logger, n=n)
    return {r[0]: r[1] for r in pool.map(__update, strategy_ids)}
