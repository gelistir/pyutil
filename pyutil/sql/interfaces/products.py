import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declared_attr, has_inherited_table
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.sql.base import Base
from pyutil.sql.interfaces.ref import _ReferenceData, Field


class HasIdMixin(object):
    @declared_attr.cascading
    def id(cls):
        if has_inherited_table(cls):
            return sq.Column(sq.ForeignKey(ProductInterface.id, onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)
        else:
            return sq.Column(sq.Integer, primary_key=True, autoincrement=True)


class MapperArgs(object):
    @declared_attr
    def __mapper_args__(cls):
        if has_inherited_table(cls):
            return {"polymorphic_identity": cls.__name__.lower()}
        else:
            return {"polymorphic_on": "discriminator"}


class TableName(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()


class ProductInterface(TableName, HasIdMixin, MapperArgs, Base):
    # note that the name should not be unique as Portfolio and Strategy can have the same name
    __name = sq.Column("name", sq.String(200), nullable=True)

    discriminator = sq.Column(sq.String)
    __table_args__ = (sq.UniqueConstraint('discriminator', 'name'),)

    _refdata = relationship(_ReferenceData, collection_class=attribute_mapped_collection("field"),
                           cascade="all, delete-orphan", back_populates="product", foreign_keys=[_ReferenceData.product_id], lazy="joined")

    reference = association_proxy('_refdata', 'value', creator=lambda k, v: _ReferenceData(field=k, content=v))

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
        # Here field can be a Field object or a string (e.g. the name of Field object)
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

    def __repr__(self):
        return "{name}".format(name=self.name)

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name

    # we want to make a set of assets, etc....
    def __hash__(self):
        return hash(self.name)

    @staticmethod
    def join_series(name):
        return "and_(ProductInterface.id==Series._product1_id, Series.name=='{name}')".format(name=name)
