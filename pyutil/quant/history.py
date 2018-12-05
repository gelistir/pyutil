import pandas as pd
from pyutil.timeseries.merge import last_index, to_datetime


def update_history(symbol, reader, t0=pd.Timestamp("2000-01-01"), offset=10):
    offset = pd.offsets.Day(n=offset)

    t = last_index(symbol.price, default=t0 + offset) - offset

    # merge new data with old existing data if it exists
    series = reader(tickers=symbol.name, t0=t).dropna()
    assert isinstance(series, pd.Series)

    if not series.empty:
       return symbol.upsert_price(ts=to_datetime(series))

