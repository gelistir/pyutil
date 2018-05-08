import pandas as _pd
import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import has_inherited_table
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.sql.base import Base
from pyutil.sql.model.ref import _ReferenceData
from pyutil.sql.model.ts import Timeseries
#from pyutil.web.aux import reset_index
#from pandasweb.frames import frame2dict


def association_table(left, right, name="association"):
    return sq.Table(name, Base.metadata,
                    sq.Column("left_id", sq.Integer, sq.ForeignKey('{left}.id'.format(left=left))),
                    sq.Column("right_id", sq.Integer, sq.ForeignKey('{right}.id'.format(right=right)))
                    )


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
    __mapper_args__ = {"polymorphic_on": discriminator}

    _refdata = relationship(_ReferenceData, collection_class=attribute_mapped_collection("field"),
                            cascade="all, delete-orphan", back_populates="product", foreign_keys=[_ReferenceData.product_id], lazy="joined")

    _timeseries = relationship(Timeseries, collection_class=attribute_mapped_collection('key'),
                               cascade="all, delete-orphan", back_populates="product", foreign_keys=[Timeseries.product_id])

    #timeseries = association_proxy('_timeseries', 'series_fast', creator=None)
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

    def get_reference(self, field, default=None):
        if field in self._refdata.keys():
            return self._refdata[field].value
        else:
            return default

    def get_timeseries(self, name, default=_pd.Series({})):
        # todo: is this efficient? maybe remove the timeseries proxy and only rely on get_timeseries?
        if name in self._timeseries.keys():
            return self._timeseries[name].series_fast
        else:
            return defaultxx


class Products(object):
    def __init__(self, products, cls, attribute="name"):
        for p in products:
            assert isinstance(p, cls)

        self.__products = {getattr(x, attribute): x for x in products}

    def __getitem__(self, item):
        return self.__products[str(item)]

    def __iter__(self):
        for symbol in self.__products.values():
            yield symbol

    @property
    def reference(self):
        x = pd.DataFrame({product: product.reference_series for product in self}).transpose()
        x.index.names = ["Product"]
        return x.rename(index=lambda x: x.name).fillna("")

    def history(self, field="PX_LAST", rename=False):
        # this could be slow
        x = pd.DataFrame({product: product.get_timeseries(name=field) for product in self})
        x.index.names = ["Date"]
        if rename:
            x = x.rename(columns=lambda x: x.name)

        return x

    def to_dict(self):
        return self.__products

    def __repr__(self):
        a = max([len(k) for k in self.__products.keys()])
        seq = ["{key:{a}.{a}}   {product}".format(key=key, product=product, a=a) for key, product in self.__products.items()]
        return "\n".join(seq)