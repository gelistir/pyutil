import pandas as pd
import logging

from pyutil.mongo.reader import _ArchiveReader
from pyutil.nav.nav import Nav


class _ArchiveWriter(_ArchiveReader):
    def __init__(self, db, logger=None):
        super(_ArchiveWriter, self).__init__(db)
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info("Archive at {0}".format(db))
        self.__db = db

    @staticmethod
    def __flatten(name, ts):
        if isinstance(ts, pd.Series):
            a = ts.copy().dropna()
            a.index = ["{0}.{1}".format(name, t.strftime("%Y%m%d")) for t in a.index]
            return {"$set": a.to_dict()}

        if isinstance(ts, pd.DataFrame):
            a = ts.copy().stack().dropna()
            a.index = ["{0}.{1}.{2}".format(name, t[1], t[0].strftime("%Y%m%d")) for t in a.index]
            return {"$set": a.to_dict()}

        raise TypeError("ts is of type {0}".format(type(ts)))

    def update_asset(self, asset, ts, name="PX_LAST"):
        # this update is cheap when ts is short!
        self.logger.debug("Asset: {0}, Name of ts: {1}, Len of ts: {2}".format(asset, name, len(ts.dropna().index)))

        # look for the asset in database
        if not ts.empty:
            m = {"_id": asset}
            self.__db.assets.update(m, {"$set": m}, upsert=True)
            self.__db.assets.update(m, self.__flatten(name, ts), upsert=True)

    def update_portfolio(self, key, portfolio, group, n=10, comment=""):
        self.logger.debug("Key {0}, Group {1}".format(key, group))

        # check if there exists an portfolio and if so update only the last n business days of this portfolio
        current_portfolio = self.portfolios[key]

        if current_portfolio:
            last = current_portfolio.index[-1]
            self.logger.debug("Last stamp {0}".format(last))
            offset = last - pd.offsets.BDay(n=n)
            self.logger.debug("Offset {0}".format(offset))
            portfolio = portfolio.truncate(before=offset)

            r = self.__flatten("returns", portfolio.nav.returns.ix[1:])
        else:
            r = self.__flatten("returns", portfolio.nav.returns)

        now = pd.Timestamp("now")
        q = {"_id": key}
        w = self.__flatten("weight", portfolio.weights)
        p = self.__flatten("price", portfolio.prices)
        self.__db.strategy.update(q, {"$set": {"group": group, "time": now, "comment": comment}}, upsert=True)
        self.__db.strategy.update(q, w, upsert=True)
        self.__db.strategy.update(q, p, upsert=True)
        self.__db.strategy.update(q, r, upsert=True)

    def update_symbols(self, frame):
        self.logger.debug("Update reference data with:\n{0}".format(frame.head(3)))
        for index, row in frame.iterrows():
            self.__db.symbol.update({"_id": index}, {"$set": row.to_dict()}, upsert=True)

    def update_rtn(self, nav, name):
        n = Nav(nav)

        for a in n.returns.index:
            assert a.weekday() <= 4

        if self.read_nav(name):
            xxx = pd.Timestamp("today").date() + pd.offsets.MonthBegin(n=-1)
            yyy = n.returns.truncate(before=xxx)
        else:
            yyy = n.returns

        m = {"_id": name}
        self.__db.fact.update(m, {"$set": m}, upsert=True)
        self.__db.fact.update(m, self.__flatten("rtn", yyy), upsert=True)

    def update_frame(self, name, frame):
        frame = frame.to_json(orient="split")
        self.__db.free.update({"_id": name}, {"_id": name, "data": frame}, upsert=True)
