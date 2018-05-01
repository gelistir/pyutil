from unittest import TestCase

import pandas as pd
from sqlalchemy.orm.exc import NoResultFound

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
        cls.s1 = Security(entity_id=1)

        cls.s1.reference[cls.f1] = "100"

        cls.c1 = Currency(name="USD")
        cls.o1 = Owner(entity_id=2, currency=cls.c1)
        cls.o1.reference[cls.f1] = "200"

        cls.session.add_all([cls.s1, cls.o1])
        cls.db = Database(session = cls.session)

    def test_securities(self):
        pdt.assert_frame_equal(self.db.securities.reference,
                               pd.DataFrame(index=[self.s1], columns=["Field A"], data=[[100]]), check_names=False)

    def test_owners(self):
        pdt.assert_frame_equal(self.db.owners.reference,
                               pd.DataFrame(index=[self.o1], columns=["Field A"], data=[[200]]), check_names=False)

    def test_owner(self):
        with self.assertRaises(NoResultFound):
            self.db.owner(entity_id=5)

        o = self.db.owner(entity_id=2)
        self.assertEqual(o.reference[self.f1], 200)

    def test_security(self):
        with self.assertRaises(NoResultFound):
            self.db.security(entity_id=5)

        s = self.db.security(entity_id=1)
        self.assertEqual(s.reference[self.f1], 100)
