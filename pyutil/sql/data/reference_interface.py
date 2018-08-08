import logging
from abc import ABC, abstractmethod

from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.model.ref import Field, FieldType
from sqlalchemy import or_


class ReferenceInterface(ABC):
    def __init__(self, session, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.session = session

    @abstractmethod
    def read(self, tickers, fields):
        pass

    def run(self):
        f = self._reference_fields
        self.logger.debug("Fields: \n{fields}".format(fields=f))

        a = self._symbol_names
        self.logger.debug("Number of Assets: \n{assets}".format(assets=len(a)))
        self.logger.debug("Latest Assets: \n{assets}".format(assets=a))

        frame = self.read(fields=f, tickers=a)  #.set_index(["ticker", "field"])["value"]
        self.logger.debug("Result (top 20): \n{frame}".format(frame=frame.head(20)))

        self._update_reference(frame=frame)

    @property
    def _reference_fields(self):
        return [field.name for field in self.session.query(Field).filter(or_(Field.type == FieldType.dynamic, Field.type == FieldType.static))]

    @property
    def _symbol_names(self):
        return [symbol.name for symbol in self.session.query(Symbol)]


    def _update_reference(self, frame):
        assets = {symbol.name: symbol for symbol in self.session.query(Symbol)}
        fields = {field.name: field for field in self.session.query(Field)}

        # that's quite ugly but doesn't make too much use of SQLalchemy's fancy associations proxies... stay away
        with self.session.no_autoflush:
            for (symbol, field), row in frame.items():
                assets[symbol].reference[fields[field]] = str(row)