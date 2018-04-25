import pandas as pd
from unittest import TestCase

from sqlalchemy.exc import IntegrityError

from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.portfolio import Portfolio
from pyutil.sql.interfaces.symbol import Symbol
from pyutil.sql.interfaces.strategy import Strategy, StrategyType
from pyutil.sql.container import Assets
from pyutil.sql.base import Base
from pyutil.sql.session import session_test
from test.config import resource, test_portfolio
import pandas.util.testing as pdt


class TestStrategy(TestCase):
    def test_strategy(self):
        with open(resource("source.py"), "r") as f:
            s = Strategy(name="peter", source=f.read(), active=True)

            # this will just return the test_portfolio
            config = s.configuration(reader=None)
            portfolio = config.portfolio

            # This will return the assets as given by the reader!
            assets = config.assets
            self.assertListEqual(assets, ["A", "B", "C", "D", "E", "F", "G"])

            assets = {asset: Symbol(bloomberg_symbol=asset) for asset in assets}

            # make a new SQL object
            #p = Portfolio(name="test")

            # store the portfolio we have just computed in there...
            s.upsert(portfolio, assets=assets)

            self.assertEqual(s.portfolio.last_valid_index(), pd.Timestamp("2015-04-22"))
            s.upsert(portfolio.tail(5), assets=assets)

            s.upsert(3*portfolio.tail(5), days=3, assets=assets)


            print(s.portfolio.weights.tail(10))

    def test_upsert(self):
        session = session_test(meta=Base.metadata, echo=True)
        s = Strategy(name="Maffay", active=True, source="", type=StrategyType.dynamic)

        # the portfolio object is defined in the __init__ of the Strategy
        self.assertIsNotNone(s._portfolio)
        #self.assertTrue(s.portfolio.empty)

        session.add(s)
        session.commit()

        # define a bunch of assets
        assets = Assets([Symbol(bloomberg_symbol=asset) for asset in ["A", "B", "C", "D", "E", "F", "G"]])
        session.add_all(assets)





        for product in session.query(ProductInterface):
            print(product.id, product.discriminator)

        x = s.upsert(test_portfolio(), assets=assets.to_dict())

        for product in session.query(ProductInterface):
            print(product.id, product.discriminator)

        session.commit()

        print(s.portfolio_id)
        print(s._portfolio.empty)

        x = s.upsert(test_portfolio(), assets=assets.to_dict())
        session.commit()

        pdt.assert_frame_equal(x.weights.rename(columns=lambda x: x.bloomberg_symbol), test_portfolio().weights)
        pdt.assert_frame_equal(x.prices.rename(columns=lambda x: x.bloomberg_symbol), test_portfolio().prices)


        # one can not create a second strategy with the same name
        s = Strategy(name="Maffay", active=True, source="", type=StrategyType.dynamic)
        session.add(s)
        with self.assertRaises(IntegrityError):
            session.commit()

        session.rollback()

        p = Portfolio(name="Maffay")
        session.add(p)
        with self.assertRaises(IntegrityError):
            session.commit()

        session.rollback()

        # one could update the Maffay portfolio without going through the strategy
        portfolio = session.query(Portfolio).filter_by(name="Maffay").one()
        portfolio.upsert_portfolio(test_portfolio(), assets=assets.to_dict())

        # we can also fish for the Strategy
        strategy = session.query(Strategy).filter_by(name="Maffay").one()
        strategy.upsert(test_portfolio(), assets=assets.to_dict())
