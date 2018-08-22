from unittest import TestCase

from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.session import postgresql_db_test
from test.config import test_portfolio


class TestAux(TestCase):
    def test_StratRunner(self):
        session = postgresql_db_test(base=Base, echo=False)

        for asset, data in test_portfolio().prices.items():
            s = Symbol(name=asset)
            s.upsert(field="PX_LAST", ts=data.dropna())
            session.add(s)

        session.commit()

