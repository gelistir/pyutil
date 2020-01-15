import datetime
import pandas as pd

from mongoengine import *


class Whoosh(Document):
    title = StringField(max_length=200, required=True)
    content = StringField(max_length=200, required=True)
    path = StringField(max_length=200, required=True)
    group = StringField(max_length=200, required=True)
    date_modified = DateTimeField(default=datetime.datetime.utcnow)

    @staticmethod
    def frame():
        frame = pd.DataFrame({n: {"group": row.group, "path": row.path, "content": row.content, "title": row.title,
                                  "created": row.date_modified} for n,row in enumerate(Whoosh.objects, start=1)}).transpose()
        frame.index.name = "Whoosh"
        frame = frame.sort_index()
        return frame
