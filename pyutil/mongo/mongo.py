from contextlib import ExitStack
from mongoengine import *


class Mongo(ExitStack):
    def __init__(self, **kwargs):
        super().__init__()
        self.__client = connect(alias="default", **kwargs)

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        disconnect(alias="default")
        return exc_type is None

