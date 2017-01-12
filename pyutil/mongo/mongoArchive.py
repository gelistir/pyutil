import collections

import pandas as pd
import logging
import warnings

from pymongo import MongoClient
from pymongo.database import Database

from ..performance.summary import fromReturns
from ..portfolio.portfolio import Portfolio

from .abcArchive import Archive

import uuid

def _f(frame):
    f = frame.copy()
    f.index = [pd.Timestamp(x) for x in f.index]
    return f


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


def _mongo(x):
    y = x.copy()
    try:
        # This is better than calling x.index.strftime directly as it also works for dates
        y.index = [a.strftime("%Y%m%d") for a in y.index]
    except AttributeError:
        warnings.warn("You are trying to convert the indizes of Pandas object into str. "
                      "They are currently {0} and of type {1}".format(x.index[0], type(x.index[0])))
        pass
    return y.to_dict()


class MongoArchive(Archive):
    class __DB(object):
        def __init__(self, db, logger):
            self.logger = logger
            self.db = db

        def keys(self):
            return self.db.distinct("_id")

        def __getitem__(self, item):
            if item in self.keys():
                return self.db.find_one({"_id": item}, {"_id": 0})
            else:
                return None

        def items(self):
            return [(k, self[k]) for k in self.keys()]

        def __setitem__(self, key, value):
            # this is implemented in the children
            raise NotImplementedError

        def __delitem__(self, key):
            #if key in self.keys():
            # could raise a KeyError
            return self.remove(key)

        def drop(self):
            return self.db.drop()

        def remove(self, key):
            return self.db.remove({"_id": key})

        def count(self):
            return self.db.count()

        @property
        def isempty(self):
            return self.db.count() == 0


    class __Assets(__DB):
        def __init__(self, db, logger=None):
            super().__init__(db=db, logger=logger)

        def update(self, asset, ts, name="PX_LAST"):
            """Update time series data for an asset"""
            self.logger.debug("Asset: {0}, Name of ts: {1}, Len of ts: {2}".format(asset, name, len(ts.dropna().index)))

            # ts empty? Get out here...
            if not ts.empty:
                m = {"_id": asset}
                # look for the asset in database
                if self.db.find_one(m):
                    # asset already in database
                    self.logger.debug({name: _mongo(ts)})
                    self.db.update(m, {"$set": _flatten({name: _mongo(ts)})}, upsert=True)
                else:
                    # asset not in the database
                    self.db.insert_one({"_id": asset, name: _mongo(ts)})

        def update_all(self, frame, name="PX_LAST"):
            """ Update assets with a frame. One asset per column"""
            for asset in frame.keys():
                self.update(asset, ts=frame[asset].dropna(), name=name)

        def __getitem__(self, item):
            d = super().__getitem__(item)
            if d:
                return {key: _f(pd.Series(values)) for key, values in d.items()}
            else:
                return None

        def frame(self, assets=None, name="PX_LAST"):
            if assets:
                # a list of items (e.g. assets) has been specified.
                for asset in assets:
                    assert asset in self.keys(), "The asset {asset} is unknown".format(asset=asset)

                p = self.db.find({"_id": {"$in": assets}, name: {"$exists":1}}, {"_id": 1, name: 1})
                # now p is a cursor, each element is a dictionary with "_id" and name as keys
                frame = pd.DataFrame({x["_id"]: pd.Series(x[name]) for x in p})
                for asset in assets:
                    if not asset in frame.keys():
                        warnings.warn("For asset {0} we could not find series {1}".format(asset, name))
            else:
                # no items (e.g. assets) have been specified. Find now all assets where name is specified for
                p = self.db.find({name: {"$exists":1}}, {"_id": 1, name: 1})
                frame = pd.DataFrame({x["_id"]: pd.Series(x[name]) for x in p})

            return _f(frame)

        def __setitem__(self, key, value):
            # value has to be a dict!
            for k in value.keys():
                self.db.insert_one({"_id": key, k: _mongo(value[k])})

    class __Symbols(__DB):
        def __init__(self, db, logger=None):
            super().__init__(db=db, logger=logger)

        def update(self, asset, dictionary):
            # this is slow if we update an empty database
            self.db.update({"_id": asset}, {"$set": _flatten(dictionary)}, upsert=True)

        def update_all(self, frame):
            for asset, row in frame.iterrows():
                self.update(asset, dictionary=row.to_dict())

        def __getitem__(self, item):
            return pd.Series(super().__getitem__(item))

        def __setitem__(self, key, value):
            # this will overwrite information
            self.db.update({"_id": key}, value, upsert=True)

        @property
        def frame(self):
            return pd.DataFrame({id: self[id] for id in self.keys()}).transpose()


    class __Portfolios(__DB):
        def __init__(self, db, logger=None):
            super().__init__(db=db, logger=logger)

        # return a dictionary portfolio
        def __getitem__(self, item):
            self.logger.debug("Portfolio: {0}".format(item))
            a = super().__getitem__(item)
            if a:
                prices = _f(pd.DataFrame(a["price"]))
                weights = _f(pd.DataFrame(a["weight"]))
                prices = prices.ix[weights.index]

                return Portfolio(prices=prices, weights=weights, logger=self.logger)
            else:
                return None

        # fast bypass to get the index underlying the portfolio
        def index(self, item):
            return self.weights(item).index

        def weights(self, item):
            p = self.db.find_one({"_id": item}, {"_id": 1, "weight": 1})
            assert p
            return _f(pd.DataFrame(p["weight"])).fillna(0.0)

        def prices(self, item):
            p = self.db.find_one({"_id": item}, {"_id": 1, "price": 1})
            assert p
            return _f(pd.DataFrame(p["price"]))

        def sector_weights(self, item, symbolmap):
            frame = self.weights(item).ffill().groupby(by=symbolmap, axis=1).sum()
            frame["total"] = frame.sum(axis=1)
            return frame

        @property
        def strategies(self):
            portfolios = self.db.find({}, {"_id": 1, "group": 1, "time": 1, "comment": 1})
            d = {p["_id"]: pd.Series({"group": p["group"], "time": p["time"], "comment": p["comment"]}) for p in portfolios}
            return pd.DataFrame(d).transpose()

        @property
        def nav(self):
            frame = pd.DataFrame({x["_id"]: fromReturns(pd.Series(x["returns"])) for x in self.db.find({}, {"_id": 1, "returns": 1})})
            return _f(frame)


        def update(self, key, portfolio, group, comment=""):
            self.logger.info("Key {0}, Group {1}".format(key, group))

            q = {"_id": key}
            if key in self.keys() and not portfolio.empty:
                # If there is any data left after the truncation process write into database
                #if not portfolio.empty:
                self.db.update(q, {"$set": _flatten({"weight": _mongo(portfolio.weights)})}, upsert=True)
                self.db.update(q, {"$set": _flatten({"price": _mongo(portfolio.prices)})}, upsert=True)
                r = portfolio.nav.pct_change().dropna()
                if len(r.index) > 0:
                    self.db.update(q, {"$set": _flatten({"returns": _mongo(r)})}, upsert=True)
            else:
                # write the entire database into the database, one has to make sure _flatten and to_json are compatible
                self.db.insert_one({"_id": key, "weight": _mongo(portfolio.weights),
                                     "price": _mongo(portfolio.prices),
                                     "returns": _mongo(portfolio.nav.pct_change().fillna(0.0))})

            now = pd.Timestamp("now")
            self.db.update(q, {"$set": {"group": group, "time": now, "comment": comment}}, upsert=True)
            return self[key]

        def __setitem__(self, key, value):
            now = pd.Timestamp("now")

            self.db.insert_one({"_id": key,
                                "weight": _mongo(value.weights),
                                "price": _mongo(value.prices),
                                "returns": _mongo(value.nav.pct_change().fillna(0.0)),
                                "time": now,
                                "group": "",
                                "comment": ""
                                })



    class __Frames(__DB):
        def __init__(self, db, logger=None):
            super().__init__(db=db, logger=logger)

        def __getitem__(self, item):
            a = super().__getitem__(item)
            if a:
                if "index" in a.keys():
                    return pd.read_json(a["data"], orient="split").set_index(a["index"])
                else:
                    return pd.read_json(a["data"], orient="split")
            else:
                return None

        def __setitem__(self, key, value):
            if len(value.index.names) > 1:
                frame=value.reset_index().to_json(orient="split")
                for name in value.index.names:
                    assert name, "Using a multiindex all levels need to have names"

                self.db.update({"_id": key}, {"_id": key, "data": frame, "index": value.index.names}, upsert=True)
            else:
                frame = value.to_json(orient="split")
                self.db.update({"_id": key}, {"_id": key, "data": frame}, upsert=True)


    def __init__(self, name=str(uuid.uuid4()), host="mongo", port=27017, logger=None):
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
    def history(self, assets=None, name="PX_LAST"):
        return self.assets.frame(assets, name)
