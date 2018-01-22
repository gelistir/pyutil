from pony import orm
from pony.orm import Database


def define_database(**db_params):
    db = Database(**db_params)
    define_entities(db)
    db.generate_mapping(create_tables=True)
    return db

def define_entities(db):
    class Type(db.Entity):
        id = orm.PrimaryKey(int, auto=True)
        name = orm.Required(str, unique=True)
        comment = orm.Optional(str)


    class Field(db.Entity):
        id = orm.PrimaryKey(int, auto=True)
        name = orm.Required(str, unique=True)
