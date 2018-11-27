from unittest import TestCase

import pandas as pd

from pyutil.sql.interfaces.series import Series
from test.test_sql.product import Product


class TestProductInterface(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p1 = Product(name="A")
        cls.p2 = Product(name="B")




