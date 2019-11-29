import logging
from functools import partial
import multiprocessing as mp

from pyutil.mongo.mongo import Mongo
from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol


def _strategy_update(strategy_id, connection_str, mongo_uri, logger=None):
    Symbol.mongo_database = Mongo(uri=mongo_uri).database
    Strategy.mongo_database = Mongo(uri=mongo_uri).database

    from pyutil.sql.session import session
    logger = logger or logging.getLogger(__name__)

    def reader(session):
        return lambda name: session.query(Symbol).filter(Symbol.name == name).one().series["PX_LAST"]

    # do a read is enough...
    with session(connection_str=connection_str) as session:
        # make fresh mongo clients to avoid forking issues
        strategy = session.query(Strategy).filter_by(id=strategy_id).one()
        logger.debug(strategy.name)

        return strategy.name, strategy.configuration(reader=reader(session)).portfolio


def run(strategies, connection_str, mongo_uri, logger=None):
    pool = mp.Pool(mp.cpu_count())
    __update = partial(_strategy_update, connection_str=connection_str, mongo_uri=mongo_uri, logger=logger)
    return {r[0]: r[1] for r in pool.map(__update, [strategy.id for strategy in strategies])}
