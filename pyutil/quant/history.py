import logging
import pandas as pd
#from pyutil.sql.interfaces.products import Timeseries
from pyutil.timeseries.merge import merge


def __read(symbol, reader, field="PX_LAST", t0=pd.Timestamp("2000-01-01"), offset=10, logger=None):
    offset = pd.offsets.Day(n=offset)

    t = (symbol.last or t0 + offset) - offset

    try:
        ts = reader(tickers=symbol.name, t0=t, field=field).dropna()

        # update to Timestamps
        ts.index = [pd.Timestamp(a) for a in ts.index]

        # merge new data with old existing data if it exists
        symbol.price = merge(new=ts, old=symbol.price)

        # return the initial time. Great for unit-testing
        return t

    except Exception as e:
        logger.warning("Problem {e} with Ticker: {ticker}".format(e=e, ticker=symbol.name))
        pass


#def __age(symbol, today=pd.Timestamp("today"), field="PX_LAST"):
#    try:
#        return (today - symbol.last(field=field)).days
#    except:
#        return None


def update_history(symbols, reader, offset=10, today=pd.Timestamp("today"), field="PX_LAST", logger=None):
    logger = logger or logging.getLogger(__name__)

    # loop over all symbols
    #for symbol in symbols:
    #    # extract data using the history function of the Bloomberg package
    return {symbol.name : __read(symbol, reader=reader, logger=logger, field=field, offset=offset) for symbol in symbols}

    #return pd.Series({symbol.name: __age(symbol, today=today, field=field) for symbol in symbols})