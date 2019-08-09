import enum as _enum

import pandas as pd
import sqlalchemy as sq
from sqlalchemy.types import Enum as _Enum

from pyutil.sql.interfaces.products import ProductInterface

class Frame(ProductInterface):
    def __init__(self, name):
        super().__init__(name)
