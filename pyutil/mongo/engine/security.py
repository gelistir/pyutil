import pandas as pd

from .pandasdocument import PandasDocument
from mongoengine import *

from .custodian import Currency


class SecurityMongo(PandasDocument):
    fullname = StringField(max_length=200)

    @staticmethod
    def reference_frame(securities):
        frame = PandasDocument.reference_frame(products=securities)
        frame["fullname"] = pd.Series({s.name: s.fullname for s in securities})
        frame.index.name = "security"
        return frame


class SecurityVolatility(PandasDocument):
    def __init__(self, security, currency):
        super().__init__(name=security.name + "_" + currency.name)
        self.security = security
        self.currency = currency

    security = ReferenceField(SecurityMongo)
    currency = ReferenceField(Currency)
