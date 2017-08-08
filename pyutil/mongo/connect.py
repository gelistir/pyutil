from mongoengine import connect


def connect_mongo(db, host, port=None, alias="default", username=None, password=None):
    return connect(db=db, host=host, port=port, alias=alias, username=username, password=password)