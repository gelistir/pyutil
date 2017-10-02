import logging
from io import BytesIO

import pandas as pd
from mongoengine import Document, StringField, DictField, BinaryField, ListField


def store(name, frame, metadata=None, logger=None):
    logger = logger or logging.getLogger(__name__)
    logger.debug("Update frame object {name}".format(name=name))
    Frame.objects(name=name).update_one(name=name, metadata=metadata, upsert=True)
    logger.debug("Store pandas object in frame object {name}".format(name=name))
    return load(name, logger=logger).put(frame=frame)


def load(name, logger=None):
    logger = logger or logging.getLogger(__name__)
    # this will return None if the Frame does not exist!
    logger.debug("Load frame object {name}".format(name=name))
    return Frame.objects(name=name).first()


def keys():
    return [frame.name for frame in Frame.objects]



class Frame(Document):
    name = StringField(required=True, max_length=200, unique=True)
    data = BinaryField()
    index = ListField()
    metadata = DictField(default={})

    @property
    def frame(self):
        """
        Return the pandas DataFrame object
        :return:
        """
        json_str = BytesIO(self.data).read().decode()
        return pd.read_json(json_str, orient="split").set_index(keys=self.index)

    def __str__(self):
        return "{name}: \n{frame}".format(name=self.name, frame=self.frame)

    def put(self, frame):
        for x in frame.index.names:
            assert x, "All columns need to have a name! {0}".format(frame.index.names)

        self.index = frame.index.names
        self.data = frame.reset_index().to_json(orient="split", date_format="iso").encode()
        self.save()
        return self.reload()

# I would love to hide this class better, can't do because Mongo wouldn't like that...
# todo: simpler serialization
class Frame2(Document):
    name = StringField(required=True, max_length=200, unique=True)
    data = StringField()
    index = ListField()
    metadata = DictField(default={})
    columns = ListField()
    index2 = ListField()
    data2 = ListField()

    @property
    def frame(self):
        """
        Return the pandas DataFrame object
        :return:
        """
        #json_str = self.data.read()
        return pd.read_json(self.data, orient="split").set_index(keys=self.index)

    def __str__(self):
        return "{name}: \n{frame}".format(name=self.name, frame=self.frame)

    def put(self, frame):
        for x in frame.index.names:
            assert x, "All columns need to have a name! {0}".format(frame.index.names)

        self.index = frame.index.names
        self.data = frame.reset_index().to_json(orient="split", date_format="iso")

        self.columns = list(frame.keys())
        self.index2 = list(frame.index)
        print([list(x) for x in frame.values])

        # self.data2 = list(frame.values)

        self.save()
        return self.reload()
