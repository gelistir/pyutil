import pandas as pd

from .pandasdocument import PandasDocument
from .custodian import Currency
from mongoengine import *


class Owner(PandasDocument):
    fullname = StringField(max_length=200)
    currency = ReferenceField(Currency)

    @classmethod
    def reference_frame(cls, products) -> pd.DataFrame:
        frame = PandasDocument.reference_frame(products=products)
        # that's why owners can't be None
        frame["Currency"] = pd.Series({owner.name: owner.currency.name for owner in products})
        frame["Entity ID"] = pd.Series({owner.name: owner.name for owner in products})
        frame["Name"] = pd.Series({owner.name: owner.fullname for owner in products})
        frame.index.name = "owner"
        return frame
#class Owner(Product, Base):
    #fullname = sq.Column("fullname", sq.String, nullable=True)

    #__currency_id = sq.Column("currency_id", sq.Integer, sq.ForeignKey(Currency.id), nullable=True)
    #__currency = _relationship(Currency, foreign_keys=[__currency_id], lazy="joined")

    #def __init__(self, name, currency=None, fullname=None):
    #    super().__init__(name=name)
    #    self.currency = currency
    #    self.fullname = fullname

    #def __repr__(self):
    #    return "Owner({id}: {fullname}, {currency})".format(id=self.name, fullname=self.fullname,
    #                                                        currency=self.currency.name)

    #@hybrid_property
    #def currency(self):
    #    return self.__currency

    #@currency.setter
    #def currency(self, value):
    #    self.__currency = value

    # @property
    # def securities(self):
    #    return set([x[0] for x in self._position.keys()])

    # @property
    # def custodians(self):
    #    return set([x[1] for x in self._position.keys()])

    # @property
    # def position_frame(self):
    #     a = pd.DataFrame(dict(self._position))
    #     if not a.empty:
    #         a = a.transpose().stack()
    #
    #         a.index.names = ["Security", "Custodian", "Date"]
    #         return a.to_frame(name="Position")
    #     else:
    #         return a
    #
    # @property
    # def vola_security_frame(self):
    #     assert self.currency, "The currency for the owner is not specified!"
    #     x = pd.DataFrame({security: security.volatility(self.currency) for security in set(self.securities)}) \
    #
    #     if not x.empty:
    #         x = x.stack()
    #         x.index.names = ["Date", "Security"]
    #         return x.swaplevel().to_frame("Volatility")
    #
    #     return x

    # @property
    # def reference_securities(self):
    #    return Security.frame(self.securities)

    # @property
    # def position_reference(self):
    #    reference = self.reference_securities
    #    position = self.position_frame
    #    volatility = self.vola_security_frame
    #    try:
    #        position_reference = position.join(reference, on="Security")
    #        return position_reference.join(volatility, on=["Security", "Date"])
    #    except (KeyError, ValueError):
    #        return pd.DataFrame({})

    # def to_json(self):
    #    ts = fromReturns(r=self._returns)
    #    return {"name": self.name, "Nav": ts, "Volatility": ts.ewm_volatility(), "Drawdown": ts.drawdown}

    # def upsert_position(self, security, custodian, ts):
    #    assert isinstance(security, Security)
    #    assert isinstance(custodian, Custodian)

    #    key = (security, custodian)
    #    self._position[key] = merge(new=ts, old=self._position.get(key, default=None))
    #    return self.position(security=security, custodian=custodian)

    # def position(self, security, custodian):
    #    return self._position[(security, custodian)]

    # def flush(self):
    #    # delete all positions...
    #    self._position.clear()

