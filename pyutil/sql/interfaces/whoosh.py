import datetime
import sqlalchemy as sq
from pyutil.sql.base import Base


class Whoosh(Base):
    __tablename__ = "whoosh"
    __searchable__ = ['title', 'content']  # these fields will be indexed by whoosh

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.Text)
    content = sq.Column(sq.Text)
    path = sq.Column(sq.Text)
    group = sq.Column(sq.Text)
    created = sq.Column(sq.DateTime, default=datetime.datetime.utcnow)

    def __repr__(self):
        return '{0}(title={1})'.format(self.__class__.__name__, self.title)
