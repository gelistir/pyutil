import collections
from builtins import AttributeError, isinstance, dict, object, property, len, type

import pandas as pd
import logging
import warnings

from pymongo import MongoClient
from pymongo.database import Database

from ..performance.summary import fromReturns
from .abc_archive import Archive
from ..portfolio.portfolio import Portfolio


def _f(frame):
    frame.index = [pd.Timestamp(x) for x in frame.index]
    return frame


def _flatten(d, parent_key=None, sep='.'):
    """ flatten dictonaries of dictionaries (of dictionaries of dict... you get it)"""
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(_flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def _mongo_series(x, format="%Y%m%d"):
    """ Convert a pandas time series into a dictionary (for Mongo)"""
    assert isinstance(x, pd.Series), "The argument is of type {0}. It has to be a Pandas Series".format(type(x))
    try:
        x.index = x.index.strftime(format)
    except AttributeError:
        pass

    return x.to_dict()


def _mongo_frame(x, format="%Y%m%d"):
    """ Convert a pandas DataFrame into a dictionary of dictionaries (for Mongo)"""
    assert isinstance(x, pd.DataFrame), "The argument is of type {0}. It has to be a Pandas DataFrame".format(type(x))
    try:
        x.index = x.index.strftime(format)
    except AttributeError:
        pass

    return {asset: _mongo_series(x[asset], format=format) for asset in x.keys()}


class ArchiveReader(Archive):

    class __Assets(object):
        def __init__(self, db, logger=None):
            self.logger = logger or logging.getLogger(__name__)
            self.__db = db


        def update(self, asset, ts, name="PX_LAST"):
            """Update time series data for an asset"""
            self.logger.debug("Asset: {0}, Name of ts: {1}, Len of ts: {2}".format(asset, name, len(ts.dropna().index)))

            # ts empty? Get out here...
            if not ts.empty:
                m = {"_id": asset}
                # look for the asset in database
                if self.__db.find_one(m):
                    # asset already in database
                    self.__db.update(m, {"$set": _flatten({name: _mongo_series(ts)})}, upsert=True)
                else:
                    # asset not in the database
                    self.__db.insert_one({"_id": asset, name: _mongo_series(ts)})

        def update_all(self, frame, name="PX_LAST"):
            """ Update assets with a frame. One asset per column"""
            for asset in frame.keys():
                self.update(asset, ts=frame[asset].dropna(), name=name)

        def __getitem__(self, item):
            a = self.__db.find_one({"_id": item}, {"_id": 0})
            return {key: _f(pd.Series(values)) for key, values in a.items()}

        def frame(self, items=None, name="PX_LAST"):
            if items:
                # a list of items (e.g. assets) has been specified.
                for item in items:
                    assert item in self.keys(), "The asset {asset} is unknown".format(asset=item)

                p = self.__db.find({"_id": {"$in": items}, name: {"$exists":1}}, {"_id": 1, name: 1})
                # now p is a cursor, each element is a dictionary with "_id" and name as keys
                frame = pd.DataFrame({x["_id"]: pd.Series(x[name]) for x in p})
                for item in items:
                    if not item in frame.keys():
                        warnings.warn("For asset {0} we could not find series {1}".format(item, name))
            else:
                # no items (e.g. assets) have been specified. Find now all assets where name is specified for
                p = self.__db.find({name: {"$exists":1}}, {"_id": 1, name: 1})
                frame = pd.DataFrame({x["_id"]: pd.Series(x[name]) for x in p})

            return _f(frame)

        def keys(self):
            return self.__db.distinct("_id")


    class __Symbols(object):
        def __init__(self, db, logger=None):
            self.logger = logger or logging.getLogger(__name__)
            self.__db = db

        def update(self, symbol, dictionary):
            # this is slow if we update an empty database
            self.__db.update({"_id": symbol}, {"$set": dictionary}, upsert=True)

        def update_all(self, frame):
            for asset, row in frame.iterrows():
                self.update(asset, dictionary=row.to_dict())

        def __getitem__(self, item):
            return pd.Series(self.__db.find_one({"_id": item}))

        @property
        def frame(self):
            return pd.DataFrame({id: self[id] for id in self.keys()}).transpose().drop("_id", axis=1)

        def keys(self):
            return self.__db.distinct("_id")


    class __Portfolios(object):
        def __init__(self, db, logger=None):
            self.logger = logger or logging.getLogger(__name__)
            self.__db = db

        def items(self):
            return [(k, self[k]) for k in self.keys()]

        def keys(self):
            return self.__db.distinct("_id")

        # return a dictionary portfolio
        def __getitem__(self, item):
            self.logger.debug("Portfolio: {0}".format(item))
            if item in self.keys():
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
            p = self.__db.find_one({"_id": item}, {"_id": 1, "weight": 1})
            assert p
            return _f(pd.DataFrame(p["weight"])).fillna(0.0)

        def prices(self, item):
            p = self.__db.find_one({"_id": item}, {"_id": 1, "price": 1})
            assert p
            return _f(pd.DataFrame(p["price"]))

        def sector_weights(self, item, symbolmap):
            frame = self.weights(item).ffill().groupby(by=symbolmap, axis=1).sum()
            frame["total"] = frame.sum(axis=1)
            return frame

        @property
        def strategies(self):
            portfolios = self.__db.find({}, {"_id": 1, "group": 1, "time": 1, "comment": 1})
            d = {p["_id"]: pd.Series({"group": p["group"], "time": p["time"], "comment": p["comment"]}) for p in portfolios}
            return pd.DataFrame(d).transpose()

        @property
        def nav(self):
            frame = pd.DataFrame({x["_id"]: fromReturns(pd.Series(x["returns"])) for x in self.__db.find({}, {"_id": 1, "returns": 1})})
            return _f(frame)


        def update(self, key, portfolio, group, comment=""):
            self.logger.info("Key {0}, Group {1}".format(key, group))

            q = {"_id": key}
            if key in self.keys() and not portfolio.empty:
                # If there is any data left after the truncation process write into database
                #if not portfolio.empty:
                self.__db.update(q, {"$set": _flatten({"weight": _mongo_frame(portfolio.weights)})}, upsert=True)
                self.__db.update(q, {"$set": _flatten({"price": _mongo_frame(portfolio.prices)})}, upsert=True)
                r = portfolio.nav.pct_change().dropna()
                if len(r.index) > 0:
                    self.__db.update(q, {"$set": _flatten({"returns": _mongo_series(r)})}, upsert=True)
            else:
                # write the entire database into the database, one has to make sure _flatten and to_json are compatible
                self.__db.insert_one({"_id": key, "weight": _mongo_frame(portfolio.weights),
                                     "price": _mongo_frame(portfolio.prices),
                                     "returns": _mongo_series(portfolio.nav.pct_change().fillna(0.0))})

            now = pd.Timestamp("now")
            self.__db.update(q, {"$set": {"group": group, "time": now, "comment": comment}}, upsert=True)
            return self[key]


    class __Frames(object):
        def __init__(self, db, logger=None):
            self.logger = logger or logging.getLogger(__name__)
            self.__db = db

        def __getitem__(self, item):
            a = self.__db.find_one({"_id": item})
            if "index" in a.keys():
                return pd.read_json(a["data"], orient="split").set_index(a["index"])
            else:
                return pd.read_json(a["data"], orient="split")

        def __setitem__(self, key, value):
            if len(value.index.names) > 1:
                frame=value.reset_index().to_json(orient="split")
                for name in value.index.names:
                    assert name, "Using a multiindex all levels need to have names"

                self.__db.update({"_id": key}, {"_id": key, "data": frame, "index": value.index.names}, upsert=True)
            else:
                frame = value.to_json(orient="split")
                self.__db.update({"_id": key}, {"_id": key, "data": frame}, upsert=True)


        def items(self):
            return [(k, self[k]) for k in self.keys()]

        def keys(self):
            return self.__db.distinct("_id")

    def __init__(self, name, host="quantsrv", port=27017, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.__db = Database(MongoClient(host, port=port), name)
        self.logger.info("Archive (read-access) at {0}".format(self.__db))
        self.portfolios = self.__Portfolios(self.__db.strategy, logger=self.logger)
        self.symbols = self.__Symbols(db=self.__db.symbols, logger=self.logger)
        self.assets = self.__Assets(db=self.__db.assets, logger=self.logger)
        self.frames = self.__Frames(db=self.__db.free, logger=self.logger)

    def __repr__(self):
        return "Reader for {0}".format(self.__db)

    # bad idea to make history a property as we may have different names, e.g PX_LAST, PX_VOLUME, etc...
    def history(self, items=None, name="PX_LAST"):
        return self.assets.frame(items, name)


