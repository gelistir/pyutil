# import logging
# from abc import ABC, abstractmethod
#
# from pyutil.sql.interfaces.products import ProductInterface
# from pyutil.sql.interfaces.symbols.frames import Frame
# from pyutil.sql.interfaces.symbols.symbol import Symbol
# from pyutil.sql.model.ref import Field, FieldType
# from sqlalchemy import or_
#
# from pyutil.sql.session import get_one_or_create
#
#
# class ReferenceInterface(ABC):
#     def __init__(self, session, logger=None):
#         self.__logger = logger or logging.getLogger(__name__)
#         self.__session = session
#
#     @staticmethod
#     @abstractmethod
#     def read_reference(tickers, fields):
#         """This method should implement how to read from a data source, e.g. Bloomberg"""
#
#     def run(self):
#         assets = {symbol.name: symbol for symbol in self.__session.query(Symbol)}
#         fields = {field.name: field for field in self.__session.query(Field).filter(or_(Field.type == FieldType.dynamic, Field.type == FieldType.static))}
#
#         for ticker, field, value in self.read_reference(fields=list(fields.keys()), tickers=list(assets.keys())):
#             assets[ticker].reference[fields[field]] = str(value)
#
#     def frame(self, name):
#         f, exists = get_one_or_create(session=self.__session, model=Frame, name=name)
#         f.frame = ProductInterface.reference_frame(self.__session.query(Symbol))
