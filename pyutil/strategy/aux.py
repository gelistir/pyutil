import logging
import multiprocessing
import os

from pyutil.influx.client import Client
from pyutil.parent import run_jobs
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.symbols.strategy import Strategy, module
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.session import get_one_or_create


def _run(strategy, symbols, logger=None):
    logger = logger or logging.getLogger(__name__)

    with Client() as client:
        logger.info("Run strategy {s}".format(s=strategy))

        ProductInterface.client = client
        configuration = strategy.configuration()
        strategy.upsert(portfolio=configuration.portfolio, symbols=symbols, days=5)


def run_strategies(session, logger=None):
    logger = logger or logging.getLogger(__name__)

    sym = _symbols(session=session)
    logger.info("{n} Symbols found".format(n=len(sym)))

    strategies = _active_strategies(session=session)
    logger.info("{n} active strategies found".format(n=len(strategies)))

    # loop over all active strategies!
    jobs = [multiprocessing.Process(target=_run, kwargs={"symbols": sym, "strategy": s, "logger": logger}) for s in
            strategies]

    run_jobs(jobs, logger=logger)


def _symbols(session):
    return {s.name: s for s in session.query(Symbol)}

def _active_strategies(session):
    return  session.query(Strategy).filter(Strategy.active==True).all()

def upsert_strategies(session, folder, logger=None):
    # set all strategies to inactive!
    logger = logger or logging.getLogger(__name__)

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

            logger.info("Strategy {s}".format(s=strat))