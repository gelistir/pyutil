import pandas as pd

from pyutil.mongo.engine.pandasdocument import PandasDocument
from mongoengine import *

from pyutil.sql.interfaces.risk.custodian import CurrencyMongo


class SecurityMongo(PandasDocument):
    fullname = StringField(max_length=200)

    #def __init__(self, name, fullname=None, **kwargs):
    #    super().__init__(str(name), **kwargs)
    #    self.fullname = fullname

    #def __repr__(self):
    #    return "Security({id}: {name})".format(id=self.name, name=self.reference["Name"])

    @staticmethod
    def reference_frame(securities, f=lambda x: x):
        frame = PandasDocument.reference_frame(products=securities, f=f)
        frame["fullname"] = pd.Series({f(s): s.fullname for s in securities})
        frame.index.name = "security"
        return frame


class SecurityVolatility(PandasDocument):
    def __init__(self, security, currency):
        super().__init__(name=security.name + "_" + currency.name)
        self.security = security
        self.currency = currency

    security = ReferenceField(SecurityMongo)
    currency = ReferenceField(CurrencyMongo)
