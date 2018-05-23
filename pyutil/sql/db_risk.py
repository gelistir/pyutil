from pyutil.sql.interfaces.risk.owner import Owner, Owners
from pyutil.sql.interfaces.risk.security import Security, Securities
from pyutil.sql.model.ref import _ReferenceData, Field
from pyutil.sql.session import session as _session

class Database(object):
    def __init__(self, session=None):
        self.__session = session or _session(db="addepar2")

    @property
    def owners(self):
        return Owners(self.__session.query(Owner))

    @property
    def securities(self):
        return Securities(self.__session.query(Security))

    #def owner(self, name):
    #    return self.__session.query(Owner).filter_by(name)
    def owner(self, value, field="Name"):
        return self.__session.query(_ReferenceData.content, Field.name, Owner)\
            .join(Owner, _ReferenceData.product_id==Owner.id)\
            .join(Field, _ReferenceData.field_id==Field.id)\
            .filter(Field.name==field)\
            .filter(_ReferenceData.content==value).one()[-1]

    def security(self, value, field="Name"):
        return self.__session.query(_ReferenceData.content, Field.name, Security)\
            .join(Security, _ReferenceData.product_id==Security.id)\
            .join(Field, _ReferenceData.field_id==Field.id)\
            .filter(Field.name==field)\
            .filter(_ReferenceData.content==value).one()[-1]

