import logging
import multiprocessing
import os

from pyutil.influx.client import Client
from pyutil.parent import run_jobs
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.symbols.strategy import Strategy, module
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.session import get_one_or_create


def _run(strategy, symbols):
    with Client() as client:

        ProductInterface.client = client

        configuration = strategy.configuration()
        strategy.upsert(portfolio=configuration.portfolio, symbols=symbols, days=5)


def run_strategies(session):
    sym = _symbols(session=session)

    strategies = _active_strategies(session=session)

    # loop over all active strategies!
    jobs = [multiprocessing.Process(target=_run, kwargs={"symbols": sym, "strategy": s}) for s in
            strategies]

    run_jobs(jobs)


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