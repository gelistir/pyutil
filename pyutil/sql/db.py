import functools

import pandas as pd

from pyutil.sql.session import session as sss


class Database(object):
    def __init__(self, session=None, db=None):
        self.__session = session or sss(db=db)

    @property
    def session(self):
        return self.__session

    @property
    def _read(self):
        return functools.partial(pd.read_sql_query, con=self.__session.bind)

    def _iter(self, cls):
        for s in self.session.query(cls):
            yield s

    def _filter(self, cls, name):
        return self.session.query(cls).filter_by(name=name).one()



#class Products(Database):
#    def __init__(self, session, discriminator):
#        super().__init__(session)
#        self.discriminator = discriminator

#    @property
#    def products(self):
#        query = "SELECT * FROM productinterface WHERE discriminator = %(name)s"
#        return self._read(query, params={"name": self.discriminator}, index_col=["id"])

    #def reference(self, type):
    #    query = "SELECT p.name as product, r.content, rf.name as field, rf.result " \
    #            "FROM reference_data r " \
    #            "JOIN reference_field rf on (rf.id = r.field_id) " \
    #            "JOIN productinterface p ON (p.id = r.product_id) " \
    #            "WHERE p.discriminator = %(name)s"

    #    frame = pd.read_sql_query(query, params={"name": type}, con=self.session.bind, index_col=["product", "field"])
    #    return reference(frame)

#    @property
#    def timeseries(self):
#        query = "SELECT p.name as product, ts.name, ts.jdata as data FROM productinterface p JOIN ts_name ts ON (ts.product_id = p.id) WHERE p.discriminator= %(name)s"
#        return self._read(query, params={"name": self.discriminator}, index_col=["product"])

