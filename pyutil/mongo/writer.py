import datetime
import pandas as pd
import logging

from pyutil.mongo.reader import _ArchiveReader
from pyutil.nav.nav import Nav

from pymongo.database import Database


class _ArchiveWriter(_ArchiveReader):
    @staticmethod
    def __flatten_dict(d):
        def items():
            for key, value in d.items():
                if isinstance(value, dict):
                    for subkey, subvalue in _ArchiveWriter.__flatten_dict(value).items():
                        yield key + "." + subkey, subvalue
                else:
                    yield key, value

        return dict(items())

    @staticmethod
    def __to_date(ts):
        if not isinstance(ts.index[0], datetime.date):
            ts.index = [a.date() for a in ts.index]
        return ts

    @staticmethod
    def __series2dict(ts, format="%Y%m%d"):
        return {t.strftime(format): y for t, y in _ArchiveWriter.__to_date(ts).dropna().iteritems()}

    @staticmethod
    def __frame2dict(frame, format="%Y%m%d"):
        return {asset: _ArchiveWriter.__series2dict(frame[asset], format=format) for asset in frame.keys()}

    def __init__(self, db, logger=None):
        super(_ArchiveWriter, self).__init__(db)
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info("Archive at {0}".format(db))
        self.__db = db

    def update_asset(self, asset, ts, name="PX_LAST"):
        # this update is cheap when ts is short!
        self.logger.debug("Asset: {0}, Name of ts: {1}, Len of ts: {2}".format(asset, name, len(ts.dropna().index)))

        # look for the asset in database
        if not ts.empty:
            m = {"id": asset,
                 "last_index": ts.last_valid_index().strftime("%Y%m%d"),
                 "last_price": ts[ts.last_valid_index()]}

            p = self.__flatten_dict({name: self.__series2dict(ts.dropna())})

            self.__db.asset.update({"id": asset}, {"$set": m}, upsert=True)
            self.__db.asset.update({"id": asset}, {"$set": p}, upsert=True)

    def update_portfolio(self, key, portfolio, group, n=10, comment=""):
        self.logger.debug("Key {0}, Group {1}".format(key, group))

        # check if there exists an portfolio and if so update only the last n business days of this portfolio
        current_portfolio = self.portfolios[key]

        if current_portfolio:
            print("FOUND PORTFOLIO")
            last = current_portfolio.index[-1]
            self.logger.debug("Last stamp {0}".format(last))
            offset = last - pd.offsets.BDay(n=n)
            self.logger.debug("Offset {0}".format(offset))
            portfolio = portfolio.truncate(before=offset)
            r = self.__flatten_dict({"returns": self.__series2dict(portfolio.nav.returns.ix[1:])})
        else:
            r = self.__flatten_dict({"returns": self.__series2dict(portfolio.nav.returns)})

        w = self.__flatten_dict({"weight": self.__frame2dict(portfolio.weights)})
        p = self.__flatten_dict({"price": self.__frame2dict(portfolio.prices)})

        m = {"group": group, "time": pd.Timestamp("now"), "comment": comment}

        self.__db.strat_new.update({"id": key}, {"$set": w}, upsert=True)
        self.__db.strat_new.update({"id": key}, {"$set": p}, upsert=True)
        self.__db.strat_new.update({"id": key}, {"$set": r}, upsert=True)
        self.__db.strat_new.update({"id": key}, {"$set": m}, upsert=True)

    def update_symbols(self, frame):
        self.logger.debug("Update reference data with:\n{0}".format(frame.head(3)))
        for asset in frame.index:
            self.__db.symbols.update({"id": asset}, {"$set": frame.ix[asset].to_dict()}, upsert=True)

    def update_rtn(self, nav, name):
        # Step 3: remove D but compare with Portfolios return!!!

        n = Nav(nav)

        for a in n.returns.index:
            assert a.weekday() <= 4

        if self.read_nav(name):
            returns_d = self.__flatten_dict({"d-rtn": self.__series2dict(n.returns.tail(25))})
        else:
            returns_d = self.__flatten_dict({"d-rtn": self.__series2dict(n.returns)})

        self.__db.factsheet.update({"fee": 0.0, "name": name}, {"$set": returns_d}, upsert=True)

    def update_frame(self, name, frame):
        frame = frame.to_json(orient="split")
        self.__db.free.update({"name": name}, {"name": name, "data": frame}, upsert=True)
