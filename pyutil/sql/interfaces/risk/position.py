from sqlalchemy.orm import relationship

from pyutil.sql.base import Base
import sqlalchemy as sq

from pyutil.sql.interfaces.risk.custodian import Custodian
from pyutil.sql.interfaces.risk.owner import Owner
from pyutil.sql.interfaces.risk.security import Security


class Position(Base):
    __tablename__ = "position"

    owner_id = sq.Column("owner_id", sq.Integer, sq.ForeignKey(Owner.id), nullable=False, primary_key=True)
    security_id = sq.Column("security_id", sq.Integer, sq.ForeignKey(Security.id), nullable=False, primary_key=True)
    custodian_id = sq.Column("custodian_id", sq.Integer, sq.ForeignKey(Custodian.id), nullable=False, primary_key=True)

    owner = relationship(Owner, foreign_keys=[owner_id], lazy="joined")
    security = relationship(Security, foreign_keys=[security_id], lazy="joined")
    custodian = relationship(Custodian, foreign_keys=[custodian_id], lazy="joined")

    date = sq.Column(sq.Date, primary_key=True)
    weight = sq.Column(sq.Float)
