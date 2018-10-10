import logging

import pandas as pd
from pyutil.sql.interfaces.symbols.frames import Frame
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.session import get_one_or_create


def __read(symbol, reader, field="PX_LAST", t0=pd.Timestamp("2000-01-01"), offset=10, logger=None):
    offset = pd.offsets.Day(n=offset)

    t = (symbol.last(field=field) or t0 + offset) - offset
    logger.debug("Symbol {symbol} and start {s}".format(symbol=symbol.name, s=t))

    try:
        # extract price from Bloomberg
        ts = reader(tickers=symbol.name, t0=t, field=field).dropna()

        logger.debug("Length of timeseries {n}".format(n=len(ts)))

        # this can now accept dates!
        symbol.upsert(ts=ts, field=field)

    except Exception as e:
        logger.warning("Problem {e} with Ticker: {ticker}".format(e=e, ticker=symbol.name))
        pass


def __age(symbol, today=pd.Timestamp("today"), field="PX_LAST"):
    try:
        return (today - symbol.last(field=field)).days
    except:
        return None


def frame(session, name="History", field="PX_LAST"):
    frm, exists = get_one_or_create(session=session, model=Frame, name=name)
    frm.frame = Symbol.frame(field)


def update_history(data, reader, offset=10, today=pd.Timestamp("today"), field="PX_LAST", logger=None):
    logger = logger or logging.getLogger(__name__)
    # loop over all symbols
    for symbol in data.symbols:
        # extract data using the history function of the Bloomberg package
        __read(symbol, reader=reader, logger=logger, field=field, offset=offset)

    return pd.Series({symbol.name: __age(symbol, today=today, field=field) for symbol in data.symbols})
