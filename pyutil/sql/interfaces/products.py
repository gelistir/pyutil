import os

import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.declarative import has_inherited_table
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.influx.client import Client
from pyutil.sql.base import Base
from pyutil.sql.model.ref import _ReferenceData, Field


def association_table(left, right, name="association"):
    return sq.Table(name, Base.metadata,
                    sq.Column("left_id", sq.Integer, sq.ForeignKey('{left}.id'.format(left=left))),
                    sq.Column("right_id", sq.Integer, sq.ForeignKey('{right}.id'.format(right=right)))
                    )

# todo: MyMixin disappear and documentation for has_inherited...
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
    # note that the name should not be unique as Portfolio and Strategy can have the same name
    __name = sq.Column("name", sq.String(200), unique=False, nullable=True)
    discriminator = sq.Column(sq.String)
    client = Client(host=os.environ["influxdb_host"], database=os.environ["influxdb_db"])

    __mapper_args__ = {"polymorphic_on": discriminator}

    _refdata = relationship(_ReferenceData, collection_class=attribute_mapped_collection("field"),
                            cascade="all, delete-orphan", back_populates="product", foreign_keys=[_ReferenceData.product_id], lazy="joined")

    reference = association_proxy('_refdata', 'value', creator=lambda k, v: _ReferenceData(field=k, content=v))

    sq.UniqueConstraint('discriminator', 'name')

    def __init__(self, name):
        self.__name = str(name)

    @hybrid_property
    def name(self):
        return self.__name

    @property
    def reference_series(self):
        return pd.Series(dict(self.reference)).rename(index=lambda x: x.name)

    @hybrid_method
    def get_reference(self, field, default=None):
        if isinstance(field, Field):
            pass
        else:
            # loop over all fields
            fields = {f.name : f for f in self._refdata.keys()}
            field = fields.get(field)

        if field in self._refdata.keys():
            return self._refdata[field].value
        else:
            return default

    def __repr__(self):
        return "{d}({name})".format(d=self.discriminator, name=self.name)

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def _ts_upsert(self, ts, field, measurement, tags=None):
        """ update a series for a field """
        if not tags:
            tags = {}

        ProductInterface.client.write_series(field=field, measurement=measurement, tags={**{"name": self.name}, **tags}, ts=ts)

    def _ts(self, field, measurement, conditions=None):
        if not conditions:
            conditions = {"name": self.name}

        return ProductInterface.client.read_series(field=field, measurement=measurement, conditions=conditions)

    def _last(self, field, measurement, conditions=None):
        if not conditions:
            conditions = {"name": self.name}

        return ProductInterface.client.last(measurement=measurement, field=field, conditions=conditions)

    @staticmethod
    def read_frame(field, measurement, conditions=None):
        # note that client is a static property of ProductInterface!
        return ProductInterface.client.read_frame(measurement=measurement, field=field, tags=["name"], conditions=conditions)

    @staticmethod
    def reference_frame(products):
        d = {s.name: {field.name: value for field, value in s.reference.items()} for s in products}
        return pd.DataFrame(d).transpose().fillna("")