from pyutil.sql.interfaces.risk.owner import Owner, Owners
from pyutil.sql.interfaces.risk.security import Security, Securities
from pyutil.sql.session import session as sss


class Database(object):
    def __init__(self, session=None):
        self.__session = session or sss(db="addepar2")

    @property
    def owners(self):
        return Owners(self.__session.query(Owner))

    @property
    def securities(self):
        return Securities(self.__session.query(Security))

    def owner(self, entity_id):
        return self.__session.query(Owner).filter_by(entity_id=entity_id).one()

    def security(self, entity_id):
        return self.__session.query(Security).filter_by(entity_id=entity_id).one()
