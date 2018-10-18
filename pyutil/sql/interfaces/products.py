import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.declarative import has_inherited_table
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

#from pyutil.influx.client import Client
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

    # this is a static variable! The system is relying on environment variables here!
    client = None #Client()

    __mapper_args__ = {"polymorphic_on": discriminator}

    _refdata = relationship(_ReferenceData, collection_class=attribute_mapped_collection("field"),
                            cascade="all, delete-orphan", back_populates="product", foreign_keys=[_ReferenceData.product_id], lazy="joined")

    reference = association_proxy('_refdata', 'value', creator=lambda k, v: _ReferenceData(field=k, content=v))

    sq.UniqueConstraint('discriminator', 'name')

    def __init__(self, name, **kwargs):
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
            fields = {f.name: f for f in self._refdata.keys()}
            field = fields.get(field)

        if field in self._refdata.keys():
            return self._refdata[field].value
        else:
            return default

    def get_ts(self, field, default=None):
        try:
            return self.ts[field]
        except KeyError:
            return None

    def __repr__(self):
        return "{d}({name})".format(d=self.discriminator, name=self.name)

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name

    # we want to make a set of assets, etc....
    def __hash__(self):
        return hash(self.name)

    #def _ts_upsert(self, ts, field, measurement, tags=None):
    #    """ update a series for a field """
    #    tags = tags or {}
    #    ProductInterface.client.write(frame=ts.to_frame(name=field), measurement=measurement, tags={**{"name": self.name}, **tags})

    #def _frame_upsert(self, frame, measurement, field_columns=None, tag_columns=None, tags=None):
    #    tags = tags or {}
    #    ProductInterface.client.write(frame=frame, field_columns=field_columns, measurement=measurement, tag_columns=tag_columns, tags={**{"name": self.name}, **tags})

    def _ts(self, field, measurement, conditions=None, tags=None):
        tags = tags or {}
        conditions = conditions or {}

        try:
            return ProductInterface.client.read(field=field, measurement=measurement, conditions={**{"name": self.name}, **conditions}, tags=tags)[field].dropna()
        except KeyError:
            return pd.Series({})

    def _last(self, field): #, measurement, conditions=None):
        try:
            self.ts[field].index[-1]
        except KeyError:
            return None

        #if not conditions:
        #    conditions = {"name": self.name}

        #return ProductInterface.client.last(measurement=measurement, field=field, conditions=conditions)

    @staticmethod
    def reference_frame(products):
        d = {s: {field.name: value for field, value in s.reference.items()} for s in products}
        return pd.DataFrame(d).transpose()


class Timeseries(Base):
    __tablename__ = "timeseries"

    __data = sq.Column("data", sq.LargeBinary)
    name = sq.Column(sq.String, primary_key=True)

    __product_id = sq.Column("product_id", sq.Integer, sq.ForeignKey("productinterface.id"), primary_key=True, index=True)


    @property
    def series(self):
        try:
            return pd.read_msgpack(self.__data)
        except ValueError:
            return pd.Series({})

    @series.setter
    def series(self, series):
        if self.__data:
            truncated = self.series
            series = pd.concat((truncated[truncated.index < series.index[0]], series))

        self.__data = series.to_msgpack()


    @property
    def last(self):
        try:
            return self.series.index[-1]
        except IndexError:
            return None


ProductInterface._timeseries = relationship(Timeseries, backref="product", collection_class=attribute_mapped_collection('name'), cascade="all, delete-orphan")


ProductInterface.ts = association_proxy('_timeseries', 'series', creator=lambda k, v: Timeseries(name=k, series=v))
