import pandas as _pd
import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.declarative import has_inherited_table
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.sql.base import Base
from pyutil.sql.model.ref import _ReferenceData, Field
from pyutil.sql.model.ts import Timeseries
from pyutil.sql.util import parse


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

    def get_timeseries(self, name, default=_pd.Series({})):
        # todo: is this efficient? maybe remove the timeseries proxy and only rely on get_timeseries?
        if name in self._timeseries.keys():
            return self._timeseries[name].series_fast
        else:
            return default

    def upsert_ts(self, name, data=None, secondary=None, tertiary=None):
        """ upsert a timeseries, get Timeseries object """

        def key(name, secondary=None, tertiary=None):
            if tertiary:
                assert secondary
                return name, secondary, tertiary

            if secondary:
                return name, secondary

            return name

        k = key(name, secondary, tertiary)

        # do we need a new timeseries object?
        if k not in self._timeseries.keys():
            self._timeseries[k] = Timeseries(name=name, product=self, secondary=secondary, tertiary=tertiary)

        # now update the timeseries object
        return self._timeseries[k].upsert(data)

    def frame(self, name, rename=False):

        x = _pd.DataFrame({x.secondary: x.series_fast for x in self._timeseries.values() if x.name == name and x.secondary}).sort_index()
        if rename:
            return x.rename(columns=lambda x: x.name)

        return x

    def __repr__(self):
        return "{d}({name})".format(d=self.discriminator, name=self.name)

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name

    def __hash__(self):
        return hash(self.name)


class Products(object):
    def __init__(self, session):
        self.session = session

    def products(self, type):
        query = "SELECT * FROM productinterface " \
                "WHERE discriminator = %(name)s"
        return pd.read_sql_query(query, params={"name": type}, con=self.session.bind, index_col=["id"])["name"]

    def reference(self, type):
        query = "SELECT p.name as product, r.content, rf.name as field, rf.result " \
                "FROM reference_data r " \
                "JOIN reference_field rf on (rf.id = r.field_id) " \
                "JOIN productinterface p ON (p.id = r.product_id) " \
                "WHERE p.discriminator = %(name)s"

        frame = pd.read_sql_query(query, params={"name": type}, con=self.session.bind, index_col=["product", "field"])
        if frame.empty:
            return pd.DataFrame(index=frame.index, columns=["value"])

        frame["value"] = frame[['result', 'content']].apply(lambda x: parse(x[1], x[0]), axis=1)
        return frame["value"].unstack()

    def timeseries(self, name):
        pass



    @property
    def x(self):
        return self.__x
# class Products(object):
#     def __init__(self, products, cls, attribute="name", f=lambda x: x):
#         for p in products:
#             assert isinstance(p, cls)
#
#         self.__products = {f(getattr(x, attribute)): x for x in products}
#
#     def __getitem__(self, item):
#         return self.__products[str(item)]
#
#     def __iter__(self):
#         for symbol in self.__products.values():
#             yield symbol
#
#     @property
#     def reference(self):
#         x = pd.DataFrame({str(key): product.reference_series for key, product in self.__products.items()}).transpose()
#         x.index.names = ["Product"]
#         return x.fillna("")
#
#     def history(self, field="PX_LAST", rename=False):
#         # this could be slow
#         x = pd.DataFrame({product: product.get_timeseries(name=field) for product in self})
#         x.index.names = ["Date"]
#         if rename:
#             x = x.rename(columns=lambda x: x.name)
#
#         return x
#
#     def to_dict(self):
#         return self.__products
#
#     def __repr__(self):
#         a = max([len(k) for k in self.__products.keys()])
#         seq = ["{key:{a}.{a}}   {product}".format(key=key, product=product, a=a) for key, product in sorted(self.__products.items())]
#         return "\n".join(seq)
#
#     def to_html(self, index_name):
#         x = self.reference.fillna("")
#
#         # every index has to be string!
#         x.index = [str(a) for a in x.index]
#         x.index.names = [index_name]
#
#         return frame2dict(x.reset_index(drop=False))




