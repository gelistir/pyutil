import pandas as _pd
import os

import sqlalchemy

from pyutil.sql.frames import Frame
from pyutil.sql.session import session as create_session

from pyutil.sql.models import Symbol as _Symbol, PortfolioSQL as _PortfolioSQL, ProductInterface


class Database(object):
    def __init__(self, session=None):
        self.__session = session or create_session(user=os.environ["reader"], password=os.environ["password_reader"], db="symbols")
        assert isinstance(self.__session, sqlalchemy.orm.session.Session), "The session is of type {t}".format(t=type(self.__session))

    @property
    def session(self):
        return self.__session

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

    def frame(self, name):
        return self.__session.query(Frame).filter_by(name=name).one().frame
