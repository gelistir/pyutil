from mongoengine import *

from .currency import Currency
from .pandasdocument import PandasDocument


class Security(PandasDocument):
    @classmethod
    def reference_frame(cls, products=None):
        products = products or Security.objects
        return super().reference_frame(products=products)



class SecurityVolatility(PandasDocument):
    #def __init__(self, security, currency):
    #    super().__init__(name=security.name + "_" + currency.name)
    #    self.security = security
    #    self.currency = currency

    security = ReferenceField(Security)
    currency = ReferenceField(Currency)
