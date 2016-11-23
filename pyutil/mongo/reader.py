import pandas as pd
import logging

from .abc_archive import Archive
from ..portfolio.portfolio import Portfolio
from ..timeseries.timeseries import adjust
from ..json.json import flatten, series2dict


def _f(frame):
    frame.index = [pd.Timestamp(x) for x in frame.index]
    return frame


class _Assets(object):
    def __init__(self, db, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.__db = db


    def update(self, asset, ts, name="PX_LAST"):
        self.logger.debug("Asset: {0}, Name of ts: {1}, Len of ts: {2}".format(asset, name, len(ts.dropna().index)))

        # look for the asset in database
        if not ts.empty:
            m = {"_id": asset}
            if self.__db.find_one(m):
                # asset already in database
                self.__db.update(m, {"$set": flatten(name, ts)}, upsert=True)
            else:
                # asset not in the database
                self.__db.update(m, {name: series2dict(ts)}, upsert=True)

    def update_all(self, frame, name="PX_LAST"):
        for asset in frame.keys():
            self.update(asset, ts=frame[asset].dropna(), name=name)

    def __getitem__(self, item):
        a = self.__db.find_one({"_id": item}, {"_id": 0})
        return {key: _f(pd.Series(values)) for key, values in a.items()}

    def frame(self, items=None, name="PX_LAST"):
        if items:
            p = self.__db.find({"_id": {"$in": items}}, {"_id": 1, name: 1})
            frame = pd.DataFrame({x["_id"]: pd.Series(x[name]) for x in p if name in x.keys()})
            for item in items:
                assert item in frame.keys(), "For asset {0} we could not find series {1}".format(item, name)

        else:
            p = self.__db.find({}, {"_id": 1, name: 1})
            frame = pd.DataFrame({x["_id"]: pd.Series(x[name]) for x in p if name in x.keys()})

        return _f(frame)


class _Symbols(object):
    def __init__(self, db, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.__db = db

    def update(self, symbol, dictionary):
        self.__db.update({"_id": symbol}, {"$set": dictionary}, upsert=True)

    def update_all(self, frame):
        for asset, row in frame.iterrows():
            self.update(asset, dictionary=row.to_dict())

    def __getitem__(self, item):
        return pd.Series(self.__db.find_one({"_id": item}))

    @property
    def frame(self):
        return pd.DataFrame({row["_id"]: pd.Series(row) for row in self.__db.find()}).transpose().drop("_id", axis=1)


class _Portfolios(object):
    def __init__(self, col, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.__col = col

    def items(self):
        return [(k, self[k]) for k in self.keys()]

    def keys(self):
        return {x["_id"] for x in self.__col.find({}, {"_id": 1})}

    # return a dictionary portfolio
    def __getitem__(self, item):
        self.logger.debug("Portfolio: {0}".format(item))
        p = self.__col.find_one({"_id": item}, {"_id": 1})
        if p:
            prices = self.prices(item)
            weights = self.weights(item)
            prices = prices.ix[weights.index]

            return Portfolio(prices=prices, weights=weights, logger=self.logger)
        else:
            return None

    # fast bypass to get the index underlying the portfolio
    def index(self, item):
        return self.weights(item).index

    def weights(self, item):
        p = self.__col.find_one({"_id": item}, {"_id": 1, "weight": 1})
        assert p
        return _f(pd.DataFrame(p["weight"])).ffill().fillna(0.0)

    def prices(self, item):
        p = self.__col.find_one({"_id": item}, {"_id": 1, "price": 1})
        assert p
        return _f(pd.DataFrame(p["price"]))

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


    def update(self, key, portfolio, group, comment=""):
        self.logger.info("Key {0}, Group {1}".format(key, group))

        q = {"_id": key}
        if key in self.keys():
            # If there is any data left after the truncation process write into database
            if not portfolio.empty:
                self.__col.update(q, {"$set": flatten("weight", portfolio.weights.stack())}, upsert=True)
                self.__col.update(q, {"$set": flatten("price", portfolio.prices.stack())}, upsert=True)
                self.__col.update(q, {"$set": flatten("returns", portfolio.nav.series.pct_change().dropna())}, upsert=True)
        else:
            # write the entire database into the database, one has to make sure _flatten and to_json are compatible
            self.__col.update(q, portfolio.to_json(), upsert=True)

        now = pd.Timestamp("now")
        self.__col.update(q, {"$set": {"group": group, "time": now, "comment": comment}}, upsert=True)
        return self[key]


class _Frames(object):
    def __init__(self, db, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.__db = db

    def __getitem__(self, item):
        a = self.__db.find_one({"_id": item}, {"data": 1})
        return pd.read_json(a["data"], orient="split")

    def __setitem__(self, key, value):
        frame = value.to_json(orient="split")
        self.__db.update({"_id": key}, {"_id": key, "data": frame}, upsert=True)


    def items(self):
        return [(k, self[k]) for k in self.keys()]


    def keys(self):
        return {x["_id"] for x in self.__db.find({}, {"_id": 1})}


class _ArchiveReader(Archive):
    def __init__(self, db, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info("Archive (read-access) at {0}".format(db))
        self.__db = db
        self.portfolios = _Portfolios(self.__db.strategy, logger=self.logger)
        self.symbols = _Symbols(db=self.__db.symbols, logger=self.logger)
        self.assets = _Assets(db=self.__db.assets, logger=self.logger)
        self.frames = _Frames(db=self.__db.free, logger=self.logger)

    def __repr__(self):
        return "Reader for {0}".format(self.__db)

    # bad idea to make history a property as we may have different names, e.g PX_LAST, PX_VOLUME, etc...
    def history(self, items=None, name="PX_LAST"):
        return self.assets.frame(items, name)