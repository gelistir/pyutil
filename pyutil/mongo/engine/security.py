import pandas as pd

from .pandasdocument import PandasDocument
from mongoengine import *

from .custodian import Currency


class SecurityMongo(PandasDocument):
    fullname = StringField(max_length=200)

    @classmethod
    def reference_frame(cls, products, f=lambda x: x.name):
        frame = PandasDocument.reference_frame(products=products, f=f)
        frame["fullname"] = pd.Series({f(s): s.fullname for s in products})
        frame.index.name = "security"
        return frame


class SecurityVolatility(PandasDocument):
    def __init__(self, security, currency):
        super().__init__(name=security.name + "_" + currency.name)
        self.security = security
        self.currency = currency

    security = ReferenceField(SecurityMongo)
    currency = ReferenceField(Currency)
