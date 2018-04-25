import sqlalchemy as sq
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.declarative import has_inherited_table

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()


class MyMixin(object):
    @declared_attr.cascading
    def id(cls):
        if has_inherited_table(cls):
            return sq.Column(sq.ForeignKey('productinterface.id'), primary_key=True)
        else:
            return sq.Column(sq.Integer, primary_key=True, autoincrement=True)


class ProductInterface(MyMixin, Base):
    __tablename__ = "productinterface"
    discriminator = sq.Column(sq.String)
    __mapper_args__ = {"polymorphic_on": discriminator}


class Portfolio(ProductInterface):
    __tablename__ = "portfolio"
    __mapper_args__ = {"polymorphic_identity": "portfolio"}
    name = sq.Column(sq.String, unique=True)
    strategy = relationship("Strategy", uselist=False, back_populates="portfolio", primaryjoin="Portfolio.id == Strategy.portfolio_id")


class Strategy(ProductInterface):
    __tablename__ = "strategy"
    name = sq.Column(sq.String(50), unique=True)
    portfolio_id = sq.Column(sq.Integer, sq.ForeignKey(Portfolio.id), nullable=False)
    portfolio = relationship(Portfolio, uselist=False, back_populates="strategy", foreign_keys=[portfolio_id])

    def __init__(self, name):
        self.name = name
        self._portfolio = Portfolio(name=self.name, strategy=self)




if __name__ == '__main__':
    engine = create_engine("sqlite://", echo=True)

    # make the tables...
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    session = sessionmaker(bind=engine)()


    s1 = Strategy(name="test")
    session.add(s1)
    session.commit()
    s1.portfolio
    session.commit()
    for x in session.query(ProductInterface):
        print(x.id, x.discriminator)