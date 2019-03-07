class Database(object):
    def __init__(self, session):
        self.__session = session

    @property
    def session(self):
        return self.__session

# extend the little Database class
#from pyutil.sql.interfaces.symbols.symbol import Symbol
#from pyutil.sql.database import Database

#class DB(Database):
#    def __init__(self, session):
#        super().__init__(session)
#
#    def symbols(self, symbols):
#        return self.session.query(Symbol).filter(Symbol.name.in_(symbols)).all()
