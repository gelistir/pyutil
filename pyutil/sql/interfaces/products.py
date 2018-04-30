import pandas as _pd
import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import has_inherited_table
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.sql.base import Base
from pyutil.sql.model.ref import _ReferenceData
from pyutil.sql.model.ts import Timeseries


class MyMixin(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    @declared_attr.cascading
    def id(cls):
        if has_inherited_table(cls):
            return sq.Column(sq.ForeignKey('productinterface.id'), primary_key=True)
        else:
            return sq.Column(sq.Integer, primary_key=True, autoincrement=True)


class ProductInterface(MyMixin, Base):

    discriminator = sq.Column(sq.String)
    __mapper_args__ = {"polymorphic_on": discriminator}

    _refdata = relationship(_ReferenceData, collection_class=attribute_mapped_collection("field"),
                            cascade="all, delete-orphan", back_populates="product", foreign_keys=[_ReferenceData.product_id], lazy="joined")

    _timeseries = relationship(Timeseries, collection_class=attribute_mapped_collection('key'),
                               cascade="all, delete-orphan", back_populates="product", foreign_keys=[Timeseries.product_id], lazy="joined")

    timeseries = association_proxy('_timeseries', 'series', creator=None)

    reference = association_proxy('_refdata', 'value', creator=lambda k, v: _ReferenceData(field=k, content=v))

    def get_reference(self, field, default=None):
        return dict(self.reference).get(field, default)

    def get_timeseries(self, name, default=_pd.Series({})):
        return dict(self.timeseries).get(name, default)

    def upsert_ts(self, name, data=None, secondary=None):
        """ upsert a timeseries, get Timeseries object """

        def key(name, secondary=None):
            if secondary:
                return name, secondary
            else:
                return name

        k = key(name, secondary)

        # do we need a new timeseries object?
        if k not in self._timeseries.keys():
            self._timeseries[k] = Timeseries(name=name, product=self, secondary=secondary)

        # now update the timeseries object
        return self._timeseries[k].upsert(data)

    def frame(self, name):
        return _pd.DataFrame({x.secondary: x.series for x in self._timeseries.values() if x.name == name and x.secondary}).sort_index()


class Products(list):
    def __init__(self, seq):
        super().__init__(seq)

    @property
    def reference(self):
        def f(ref):
            return pd.Series(dict(ref)).rename(index=lambda x: x.name)

        x = pd.DataFrame({product: f(product.reference) for product in self}).transpose()
        x.index.names = ["Product"]
        return x

    def history(self, field="PX_LAST"):
        # this could be slow
        x = pd.DataFrame({product: product.get_timeseries(name=field) for product in self})
        x.index.names = ["Date"]
        return x