from unittest import TestCase

import pandas as pd

from pyutil.sql.base import Base
from pyutil.sql.db_risk import Database
from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.owner import Owner
from pyutil.sql.interfaces.risk.security import Security
from pyutil.sql.model.ref import Field, DataType, FieldType
from pyutil.sql.session import session_test

import pandas.util.testing as pdt


class TestDatabaseRisk(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = session_test(meta=Base.metadata, echo=False)
        cls.f1 = Field(name="Field A", result=DataType.integer, type=FieldType.dynamic)
        cls.s1 = Security(name=1)

        cls.s1.reference[cls.f1] = "100"

        cls.c1 = Currency(name="USD")
        cls.o1 = Owner(name=2, currency=cls.c1)
        cls.o1.reference[cls.f1] = "200"

        cls.session.add_all([cls.s1, cls.o1])
        cls.db = Database(session = cls.session)

    def test_securities(self):
        pdt.assert_frame_equal(self.db.securities.reference(rename=False),
                               pd.DataFrame(index=[self.s1], columns=[self.f1], data=[[100]]), check_names=False)

    def test_owners(self):
        print(self.db.owners.reference)

        pdt.assert_frame_equal(self.db.owners.reference(rename=False),
                               pd.DataFrame(index=[self.o1], columns=[self.f1], data=[[200]]), check_names=False)
