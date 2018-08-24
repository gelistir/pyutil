import logging
import pandas as pd

from pyutil.sql.interfaces.symbols.frames import Frame
from pyutil.sql.interfaces.symbols.symbol import Symbol

from abc import ABC, abstractmethod

from pyutil.sql.session import get_one_or_create


class HistoryInterface(ABC):
    def __init__(self, session, logger=None):
        self.__session = session
        self.__logger = logger or logging.getLogger(__name__)

    @staticmethod
    @abstractmethod
    def read_history(ticker, t0, field):
        """This method should implement how read from a data source, e.g. Bloomberg"""

    def run(self, t0=pd.Timestamp("2000-01-01"), offset=10, field="PX_LAST"):
        offset = pd.offsets.Day(n=offset)

        for n, symbol in enumerate(self.__session.query(Symbol)):
            try:
                t = (symbol.last(field=field) or t0 + offset) - offset
                self.__logger.debug("Symbol {symbol} and start {s}".format(symbol=symbol.name, s=t))

                # extract price from Bloomberg
                ts = self.read_history(ticker=symbol.name, t0=t, field=field).dropna()

                self.__logger.debug("Length of timeseries {n}".format(n=len(ts)))

                # this can now accept dates!
                symbol.upsert(ts=ts, field=field)

            except Exception as e:
                self.__logger.warning("Problem {e} with Ticker: {ticker}".format(e=e, ticker=symbol.name))
                pass

    def age(self, today=pd.Timestamp("today"), field="PX_LAST"):
        def f(symbol):
            try:
                return (today - symbol.last(field=field)).days
            except:
                return None

        return pd.Series({symbol.name: f(symbol) for symbol in self.__session.query(Symbol)})

    def frame(self, name, field="PX_LAST"):
        f, exists = get_one_or_create(session=self.__session, model=Frame, name=name)
        f.frame = Symbol.frame(field)