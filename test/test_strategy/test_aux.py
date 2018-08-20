from unittest import TestCase

import pandas.util.testing as pdt

from pyutil.sql.base import Base
from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.session import postgresql_db_test
from pyutil.strategy.aux import upsert_strategies, run_strategies
from test.config import test_portfolio, resource


class TestConfigMaster(TestCase):
    @classmethod
    def setUpClass(cls):
        # create a session
        cls.session = postgresql_db_test(base=Base, echo=False)

        for asset, data in test_portfolio().prices.items():
            s = Symbol(name=asset)
            s.upsert(field="PX_LAST", ts=data.dropna())
            cls.session.add(s)

        folder = resource("strat")

        upsert_strategies(session=cls.session, folder=folder)

        cls.session.commit()


    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def test_run(self):
        run_strategies(session=self.session)

        strats = [s for s in self.session.query(Strategy)]
        assert len(strats) == 2

        for s in strats:
            pdt.assert_frame_equal(s.portfolio.weights, test_portfolio().weights[s.portfolio.assets], check_names=False)
            pdt.assert_frame_equal(s.portfolio.prices, test_portfolio().prices[s.portfolio.assets], check_names=False)


