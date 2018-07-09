from pyutil.performance.summary import fromNav
from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.sql.interfaces.products import ProductInterface


class Portfolio(ProductInterface):
    __mapper_args__ = {"polymorphic_identity": "portfolio"}

    def __init__(self, name):
        super().__init__(name)

    def last(self, client):
        try:
            return client.query("""SELECT LAST({f}) FROM "portfolio" where "portfolio"='{n}'""".format(n=self.name, f="weight"))["portfolio"].index[0].date()
        except KeyError:
            return None

    def upsert_influx(self, client, portfolio):
        assert isinstance(portfolio, _Portfolio)

        for symbol in portfolio.assets:
            client.series_upsert(ts=portfolio.weights[symbol].dropna(), field="weight", tags={"portfolio": self.name, "asset": symbol}, series_name="portfolio")
            client.series_upsert(ts=portfolio.prices[symbol].dropna(), field="price", tags={"portfolio": self.name, "asset": symbol}, series_name="portfolio")

        # it's important to recompute the entire portfolio here...
        p = self.portfolio_influx(client=client)
        client.series_upsert(ts=p.nav, field="nav", tags={"portfolio": self.name}, series_name="portfolio")
        client.series_upsert(ts=p.leverage, field="leverage", tags={"portfolio": self.name}, series_name="portfolio")

    def portfolio_influx(self, client):
        w = client.frame(field="weight", measurement="portfolio", tags=["portfolio", "asset"], conditions=[("portfolio", self.name)])
        w.index = w.index.droplevel("portfolio")

        p = client.frame(field="price", measurement="portfolio", tags=["portfolio", "asset"], conditions=[("portfolio", self.name)])
        p.index = p.index.droplevel("portfolio")

        return _Portfolio(prices=p, weights=w)

    def symbols_influx(self, client):
        return client.tags(measurement="portfolio", key="asset", conditions=[("portfolio", self.name)])

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
        return fromNav(client.series(field="nav", measurement="portfolio", conditions=[("portfolio", self.name)]))

    def leverage(self, client):
        return client.series(field="leverage", measurement="portfolio", conditions=[("portfolio", self.name)])
