import logging
import pandas as pd

from pyutil.sql.interfaces.symbols.symbol import Symbol

from abc import ABC, abstractmethod

class HistoryInterface(ABC):
    # extract prices from Bloomberg
    def __init__(self, session, logger=None):
        self.__session = session
        self.__logger = logger or logging.getLogger(__name__)

    @abstractmethod
    def read(self, ticker, t0, field):
        pass

    def run(self, t0=pd.Timestamp("2000-01-01"), offset=10, field="PX_LAST"):
        offset = pd.offsets.Day(n=offset)

        for n, symbol in enumerate(self.__session.query(Symbol)):
            try:
                t = (symbol.last(field=field) or t0 + offset) - offset
                self.__logger.debug("Symbol {symbol} and start {s}".format(symbol=symbol.name, s=t))

                # extract price from Bloomberg
                ts = self.read(ticker=symbol.name, t0=t, field=field).dropna()

                self.__logger.debug("Length of timeseries {n}".format(n=len(ts)))

                # this can now accept dates!
                symbol.upsert(ts=ts, field=field)

            except Exception as e:
                self.__logger.warning("Problem {e} with Bloomberg. Ticker: {ticker}".format(e=e, ticker=symbol.name))
                pass

    def age(self, today=pd.Timestamp("today"), field="PX_LAST"):
        return pd.Series({symbol.name: (today - symbol.last(field=field)).days for symbol in session.query(Symbol)})