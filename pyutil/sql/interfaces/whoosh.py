import datetime
import pandas as pd

import sqlalchemy as sq
from pyutil.sql.base import Base


class Whoosh(Base):
    __tablename__ = "whoosh"
    __searchable__ = ['title', 'content']  # these fields will be indexed by whoosh

    id = sq.Column(sq.Integer, primary_key=True, autoincrement=True, nullable=False)
    title = sq.Column(sq.Text, nullable=False)
    content = sq.Column(sq.Text, nullable=False)
    path = sq.Column(sq.Text, nullable=False)
    group = sq.Column(sq.Text, nullable=False)
    created = sq.Column(sq.DateTime, default=datetime.datetime.utcnow)

    def __init__(self, title, content, path, group):
        self.title = title
        self.content = content
        self.path = path
        self.group = group

    def __repr__(self):
        return '{0}(title={1})'.format(self.__class__.__name__, self.title)

    @staticmethod
    def frame(rows):
        frame = pd.DataFrame({n: {"group": row.group, "path": row.path, "content": row.content, "title": row.title,
                                  "created": row.created} for n,row in enumerate(rows)}).transpose()
        frame.index.name = "Symbol"
        frame = frame.sort_index()
        return frame

