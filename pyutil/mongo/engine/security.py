import pandas as pd

from .pandasdocument import PandasDocument
from mongoengine import *

from .currency import Currency


class Security(PandasDocument):
    fullname = StringField(max_length=200)

    @classmethod
    def reference_frame(cls, products=None):
        products = products or Security.objects
        frame = super().reference_frame(products=products)
        frame["fullname"] = pd.Series({s.name: s.fullname for s in products})
        return frame


class SecurityVolatility(PandasDocument):
    def __init__(self, security, currency):
        super().__init__(name=security.name + "_" + currency.name)
        self.security = security
        self.currency = currency

    security = ReferenceField(Security)
    currency = ReferenceField(Currency)
