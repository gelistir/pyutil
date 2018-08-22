from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class User(Base):
    __tablename__ = "user"
    name = Column(String, primary_key=True, unique=True)

    def __init__(self, name):
        self.name = name