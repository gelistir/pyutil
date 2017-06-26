import pandas as pd
from mongoengine import Document, StringField, DictField


class Frame(Document):
    name = StringField(required=True, max_length=200, unique=True)
    data = DictField()

    @property
    def frame(self):
        return pd.DataFrame(self.data)

    def __str__(self):
        return self.frame
