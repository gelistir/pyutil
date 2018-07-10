from pyutil.performance.summary import fromNav
from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.sql.interfaces.products import ProductInterface


class Portfolio(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "portfolio"}

    def __init__(self, name):
        super().__init__(name)

    def last(self, client):
        try:
            return client.query(""" SELECT LAST({f}) FROM "nav" """.format(f=self.name))["nav"].index[0].date()
        except KeyError:
            return None

    def upsert_influx(self, client, portfolio):
        assert isinstance(portfolio, _Portfolio)
        client.write_portfolio(portfolio=portfolio, name=self.name)

        # it's important to recompute the entire portfolio here...
        p = self.portfolio_influx(client=client)
        print(p.nav.index[0])

        # update the nav
        client.write_series(ts=p.nav.dropna(), field=self.name, measurement="nav")
        # update the leverage
        client.write_series(ts=p.leverage.dropna(), field=self.name, measurement="leverage")

    def portfolio_influx(self, client):
        p, w = client.read_portfolio(name=self.name)
        return _Portfolio(prices=p, weights=w)

    def symbols_influx(self, client):
        # todo: change to tag values!
        return self.portfolio_influx(client=client).assets
        #return client.tag_values(measurement="prices", key="name", conditions=[("portfolio", self.name)])

    # we have fast views to extract data... All the functions below are not required...
    def portfolio(self, rename=False):
        # does it work?
        """ this we need to read the old format """
        prices = self.frame("price")
        weights = self.frame("weight")

        if rename:
            prices.rename(columns=lambda x: x.name, inplace=True)
            weights.rename(columns=lambda x: x.name, inplace=True)

        return _Portfolio(prices=prices, weights=weights)

    def nav(self, client):
        return fromNav(client.read_series(field=self.name, measurement="nav"))

    def leverage(self, client):
        return client.read_series(field=self.name, measurement="leverage")

    @staticmethod
    def nav_all(client):
        return client.read_frame(measurement="nav")

    @staticmethod
    def leverage_all(client):
        return client.read_frame(measurement="leverage")
