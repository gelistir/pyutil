from pyutil.sql.session import session_test

from pyutil.sql.base import Base
from test.config import resource

#session = session_test(meta=Base.metadata)
#connection = session.connection()

with open(resource("views.ddl"), "r") as f:
    views = [a for a in f.read().split("\n\n")]

print(views)
session = session_test(meta=Base.metadata, file="wurst5.db")

#    for a in f.read().split("\n\n"):
#        connection.execute(a)