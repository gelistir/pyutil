import logging
import pandas as pd
from mongoengine import Document, StringField, FileField, DictField

from io import BytesIO


def store(name, pandas_object, metadata=None, logger=None):
    logger = logger or logging.getLogger(__name__)
    logger.debug("Update frame object {name}".format(name=name))
    Frame.objects(name=name).update_one(name=name, metadata=metadata, upsert=True)
    logger.debug("Store pandas object in frame object {name}".format(name=name))
    return load(name, logger=logger).put(frame=pandas_object)


def load(name, logger=None):
    logger = logger or logging.getLogger(__name__)
    # this will return None if the Frame does not exist!
    logger.debug("Load frame object {name}".format(name=name))
    return Frame.objects(name=name).first()


def keys():
    return [frame.name for frame in Frame.objects]


# I would love to hide this class better, can't do because Mongo wouldn't like that...
class Frame(Document):
    name = StringField(required=True, max_length=200, unique=True)
    data = BinaryField()
    metadata = DictField(default={})

    def __decode(self):
        return BytesIO(self.data).read().decode()

    def __read_json(self, typ="frame"):
        return pd.read_json(self.__decode(), typ=typ, orient="split")

    @property
    def frame(self):
        """
        Return the pandas DataFrame object
        :return:
        """
        return self.__read_json(typ="frame")

    @property
    def series(self):
        """
        Return the pandas Series object
        :return:
        """
        return self.__read_json(typ="series")

    def __str__(self):
        return "{name}: \n{frame}".format(name=self.name, frame=self.frame)

    def put(self, frame):
        self.data = frame.to_json(orient="split").encode()
        self.save()
        return self.reload()