import pandas as pd

from pyutil.performance.summary import fromNav
from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.symbols.symbol import Symbol


class Portfolio(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "portfolio"}

    def __init__(self, name):
        super().__init__(name)

    def last(self):
        return Portfolio.client.last(measurement="nav", field="nav", conditions={"name": self.name})

    def upsert_influx(self, portfolio):
        assert isinstance(portfolio, _Portfolio)

        ww = portfolio.weights.stack().to_frame(name="Weight")
        pp = portfolio.prices.stack().to_frame(name="Price")

        ww.index.names = ["Date", "Asset"]
        pp.index.names = ["Date", "Asset"]
        xx = pd.concat([ww, pp], axis=1).reset_index().set_index("Date")

        Portfolio.client.write_points(xx, measurement="xxx2", tag_columns=["Asset"], field_columns=["Weight", "Price"], tags={"Portfolio": self.name}, batch_size=10000, time_precision="s")

        portfolio_new = self.portfolio_influx

        # todo: drop data first...
        # update the nav
        Portfolio.client.write_series(ts=portfolio_new.nav.dropna(), field="nav", tags={"name": self.name}, measurement="nav")
        # update the leverage
        Portfolio.client.write_series(ts=portfolio_new.leverage.dropna(), field="leverage", tags={"name": self.name}, measurement="leverage")

        # upsert sector


        return portfolio_new

    @property
    def portfolio_influx(self):
        p = Portfolio.client.read_series(measurement="xxx2", field="Price", tags=["Asset"], conditions={"Portfolio": self.name}, unstack=True)
        w = Portfolio.client.read_series(measurement="xxx2", field="Weight", tags=["Asset"], conditions={"Portfolio": self.name}, unstack=True)
        return _Portfolio(prices=p, weights=w)

    @property
    def symbols_influx(self):
        """ Those are the names """
        return self.portfolio_influx.assets

    @property
    def nav(self):
        return fromNav(Portfolio.client.read_series(field="nav", measurement="nav", conditions={"name": self.name}))

    @property
    def leverage(self):
        return Portfolio.client.read_series(field="leverage", measurement="leverage", conditions={"name": self.name})

    @staticmethod
    def nav_all():
        return Portfolio.client.read_series(measurement="nav", field="nav", tags=["name"], unstack=True)

    @staticmethod
    def leverage_all():
        return Portfolio.client.read_series(measurement="leverage", field="leverage", tags=["name"], unstack=True)

    def symbols(self, session):
        return [session.query(Symbol).filter_by(name=asset).one() for asset in self.symbols_influx]

    def sector(self, session, total=False):
        symbolmap = Symbol.sectormap(self.symbols(session=session))
        return self.portfolio_influx.sector_weights(symbolmap=symbolmap, total=total)
