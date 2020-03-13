from mongoengine import *

from .currency import Currency
from .pandasdocument import PandasDocument


class Security(PandasDocument):
    @classmethod
    def reference_frame(cls, products=None):
        products = products or Security.objects
        return super().reference_frame(products=products)


class SecurityVolatility(PandasDocument):
    security = ReferenceField(Security)
    currency = ReferenceField(Currency)
