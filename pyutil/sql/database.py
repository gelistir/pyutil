import pandas as _pd

from pyutil.sql.frames import Frame
from pyutil.sql.models import Symbol as _Symbol, PortfolioSQL as _PortfolioSQL
from pyutil.sql.products import ProductInterface


def _session_read():
    import os
    from sqlalchemy import create_engine as _create_engine
    from sqlalchemy.orm import sessionmaker as _sessionmaker

    engine = _create_engine('postgresql+psycopg2://{user}:{password}@quantsrv/symbols'.format(user=os.environ["reader"],
                                                                                              password=os.environ[
                                                                                                  "password_reader"]))
    return _sessionmaker(bind=engine)()


class Database(object):
    def __init__(self, session=None):
        self.__session = session or _session_read()

    @property
    def session(self):
        return self.__session

    #def asset(self, name):
    #    return self.__session.query(_Symbol).filter_by(bloomberg_symbol=name).one()

    def history(self, cls=None, field="PX_LAST", products=None):
        if products:
            return _pd.DataFrame({product: product.timeseries[field] for product in products})
        else:
            return _pd.DataFrame({product: product.timeseries[field] for product in
                                  self.session.query(ProductInterface).with_polymorphic(cls)})

    def reference(self, cls=None, products=None):
        if products:
            x = _pd.DataFrame({product: _pd.Series(product.reference) for product in products}).transpose()
        else:
            x = _pd.DataFrame({product: _pd.Series(product.reference) for product in
                               self.__session.query(ProductInterface).with_polymorphic(cls)}).transpose()

        x.index.name = "Asset"
        return x

    def portfolios(self, names=None):
        if names:
            return {name: self.__session.query(_PortfolioSQL).filter_by(name=name).one() for name in names}
        else:
            return {portfolio.name: portfolio for portfolio in self.__session.query(_PortfolioSQL)}

    def mtd(self, names=None):
        frame = _pd.DataFrame(
            {name: portfolio.nav.mtd_series for name, portfolio in self.portfolios(names).items()}).sort_index(
            ascending=False)
        frame.index = [a.strftime("%b %d") for a in frame.index]
        frame = frame.transpose()
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    def ytd(self, names=None):
        frame = _pd.DataFrame(
            {name: portfolio.nav.ytd_series for name, portfolio in self.portfolios(names).items()}).sort_index(
            ascending=False)
        frame.index = [a.strftime("%b") for a in frame.index]
        frame = frame.transpose()
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    def sector(self, names=None):
        def f(frame):
            return frame.iloc[-1]

        map = {symbol.bloomberg_symbol: symbol.group.name for symbol in self.__session.query(_Symbol)}

        frame = _pd.DataFrame(
            {name: f(portfolio.sector(map=map)) for name, portfolio in self.portfolios(names).items()}).sort_index(
            ascending=False)
        frame = frame.transpose()
        frame["total"] = frame.sum(axis=1)
        return frame

    def recent(self, names=None, n=15):
        frame = _pd.DataFrame(
            {name: portfolio.nav.recent() for name, portfolio in self.portfolios(names).items()}).sort_index(
            ascending=False)
        frame.index = [a.strftime("%b %d") for a in frame.index]
        frame = frame.head(n)
        frame = frame.transpose()
        frame["total"] = (frame + 1).prod(axis=1) - 1
        return frame

    def period_returns(self, names=None):
        frame = _pd.DataFrame(
            {name: portfolio.nav.period_returns for name, portfolio in self.portfolios(names).items()}).sort_index(
            ascending=False)
        return frame.transpose()

    def performance(self, names=None):
        frame = _pd.DataFrame(
            {name: portfolio.nav.summary() for name, portfolio in self.portfolios(names).items()}).sort_index(
            ascending=False)
        return frame.transpose()

    # def strategies(self):
    #    return self.__session.query(_Strategy)

    def frame(self, name):
        return self.__session.query(Frame).filter_by(name=name).one().frame
