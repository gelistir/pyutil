import sqlalchemy as sq
from pyutil.sql.interfaces.products import Base


def association_table(left, right, name="association"):
    return sq.Table(name, Base.metadata,
                    sq.Column("left_id", sq.Integer, sq.ForeignKey('{left}.id'.format(left=left))),
                    sq.Column("right_id", sq.Integer, sq.ForeignKey('{right}.id'.format(right=right)))
                    )
