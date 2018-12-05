import logging
import pandas as pd
from pyutil.timeseries.merge import last_index, to_datetime


def update_history(symbol, reader, t0=pd.Timestamp("2000-01-01"), offset=10, logger=None):
    logger = logger or logging.getLogger(__name__)

    offset = pd.offsets.Day(n=offset)

    t = last_index(symbol.price, default=t0 + offset) - offset

    try:
        # merge new data with old existing data if it exists
        return symbol.upsert_price(ts=to_datetime(reader(tickers=symbol.name, t0=t).dropna()))

    except Exception as e:
        logger.warning("Problem {e} with Ticker: {ticker}".format(e=e, ticker=symbol.name))
        pass
