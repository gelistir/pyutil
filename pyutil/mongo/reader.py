import abc
import pandas as pd
import logging
from pyutil.nav.nav import Nav

from pyutil.portfolio.portfolio import Portfolio
from pyutil.timeseries.timeseries import adjust


def _f(frame):
    frame.index = [pd.Timestamp(x) for x in frame.index]
    return frame


class Archive(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def history(self, items, name, before):
        return


class _Portfolios(object):
    def __init__(self, col, logger=None):
        self.__logger = logger or logging.getLogger(__name__)
        self.__col = col

    def items(self):
        return [(k, self[k]) for k in self.keys()]

    def keys(self):
        return {x["_id"] for x in self.__col.find({}, {"_id": 1})}

    # return a dictionary portfolio
    def __getitem__(self, item):
        self.__logger.debug("Portfolio: {0}".format(item))
        p = self.__col.find_one({"_id": item}, {"_id": 1, "price": 1, "weight": 1})
        if p:
            prices = _f(pd.DataFrame(p["price"]))
            weights = _f(pd.DataFrame(p["weight"])).fillna(0.0)
            prices = prices.ix[weights.index]

            return Portfolio(prices=prices, weights=weights)
        else:
            return None

    # fast bypass to get the index underlying the portfolio
    def index(self, item):
        return self.weights(item).index

    def weights(self, item):
        p = self.__col.find_one({"_id": item}, {"_id": 1, "weight": 1})
        assert p
        return _f(pd.DataFrame(p["weight"])).ffill().fillna(0.0)

    def sector_weights(self, item, symbolmap):
        frame = self.weights(item).ffill().groupby(by=symbolmap, axis=1).sum()
        frame["total"] = frame.sum(axis=1)
        return frame

    @property
    def strategies(self):
        portfolios = self.__col.find({}, {"_id": 1, "group": 1, "time": 1, "comment": 1})
        d = {p["_id"]: pd.Series({"group": p["group"], "time": p["time"], "comment": p["comment"]}) for p in portfolios}
        return pd.DataFrame(d).transpose()

    @property
    def nav(self):
        frame = pd.DataFrame({x["_id"]: pd.Series(x["returns"]) for x in self.__col.find({}, {"_id": 1, "returns": 1})})
        return _f(frame + 1.0).cumprod().apply(adjust)


class _ArchiveReader(Archive):
    def __init__(self, db, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info("Archive (read-access) at {0}".format(db))
        self.__db = db
        self.__portfolio = _Portfolios(db.strategy)

    def __repr__(self):
        return "Reader for {0}".format(self.__db)

    @property
    def portfolios(self):
        return self.__portfolio

    # bad idea to make history a property as we may have different names, e.g PX_LAST, PX_VOLUME, etc...
    def history(self, items=None, name="PX_LAST", before=pd.Timestamp("2002-01-01")):
        collection = self.__db.assets

        if items:
            p = collection.find({"_id": {"$in": items}}, {"_id": 1, name: 1})
            frame = pd.DataFrame({x["_id"]: pd.Series(x[name]) for x in p if name in x.keys()})
            for item in items:
                assert item in frame.keys(), "For asset {0} we could not find series {1}".format(item, name)

        else:
            p = collection.find({}, {"_id": 1, name: 1})
            frame = pd.DataFrame({x["_id"]: pd.Series(x[name]) for x in p if name in x.keys()})

        return _f(frame).truncate(before=before)

    def history_series(self, item, name="PX_LAST"):
        return self.history(items=[item], name=name)[item]

    @property
    def symbols(self):
        f = pd.DataFrame({row["_id"]: pd.Series(row) for row in self.__db.symbol.find()}).transpose()
        return f.drop("_id", axis=1)

    def read_nav(self, name):
        a = self.__db.fact.find_one({"_id": name}, {"rtn": 1})
        if a:
            y = _f(pd.Series(a["rtn"]))
            return Nav((y + 1.0).cumprod())
        else:
            return None

    def read_frame(self, name=None):
        if name:
            a = self.__db.free.find_one({"_id": name}, {"data": 1})
            return pd.read_json(a["data"], orient="split")
        else:
            return {p["_id"]: pd.read_json(p["data"], orient="split") for p in self.__db.free.find()}



