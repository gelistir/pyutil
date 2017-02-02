import collections

import pandas as pd
import logging
import warnings

from pymongo import MongoClient
from pymongo.database import Database

from pyutil.mongo.asset import Asset
from pyutil.mongo.portfolios import Portfolios
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


def _to_dict(data):
    if isinstance(data, pd.DataFrame):
        return {k: v.dropna().to_dict() for k,v in data.items()}
    else:
        return data.to_dict()


def _mongo(x):
    y = x.copy()
    try:
        # This is better than calling x.index.strftime directly as it also works for dates
        y.index = [a.strftime("%Y%m%d") for a in y.index]
    except AttributeError:
        warnings.warn("You are trying to convert the indizes of Pandas object into str. "
                      "They are currently {0} and of type {1}".format(x.index[0], type(x.index[0])))
        pass

    return _to_dict(data=y)


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
            for k in self.keys():
                yield (k, self[k])

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
        def empty(self):
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

            # delete rows that are all NaN...
            return _f(frame).dropna(how="all", axis=0)

        def __setitem__(self, key, value):
            raise NotImplementedError

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
            return pd.DataFrame({id: x for id, x in self.items()}).transpose()

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
                del a["price"]
                del a["weight"]

                return Portfolio(prices=prices, weights=weights, **a)
            else:
                return None

        def update(self, key, portfolio):
            self.logger.info("Key {0}".format(key))

            q = {"_id": key}
            if key in self.keys() and not portfolio.empty:
                # If there is any data left after the truncation process write into database
                self.db.update(q, {"$set": _flatten({"weight": _mongo(portfolio.weights)})}, upsert=True)
                self.db.update(q, {"$set": _flatten({"price": _mongo(portfolio.prices)})}, upsert=True)
            else:
                # write the entire database into the database, one has to make sure _flatten and to_json are compatible
                self.db.insert_one({"_id": key, "weight": _mongo(portfolio.weights), "price": _mongo(portfolio.prices)})

            if portfolio.meta:
                self.db.update(q, {"$set": portfolio.meta}, upsert=True)

            return self[key]

        def __setitem__(self, key, value):
            raise NotImplementedError

    def __init__(self, name=str(uuid.uuid4()), host="mongo", port=27017, user=None, password=None, logger=None):
        """
        Mongo Archive for data
        :param name: Name of the Mongo Database, e.g. "production"
        :param host: Host
        :param port: Port
        :param logger: Logger
        """
        self.logger = logger or logging.getLogger(__name__)
        self.__db = Database(MongoClient(host, port=port), name)

        if user and password:
            self.__db.authenticate(name=user, password=password)

        self.logger.info("Archive (read-access) at {0}".format(self.__db))

        # the database will have (at least) 4 collections.
        self.portfolios = self.__Portfolios(self.__db.strategy, logger=self.logger)
        self.symbols = self.__Symbols(db=self.__db.symbols, logger=self.logger)
        self.assets = self.__Assets(db=self.__db.assets, logger=self.logger)

    def __repr__(self):
        return "Reader for {0}".format(self.__db)

    # bad idea to make history a property as we may have different names, e.g PX_LAST, PX_VOLUME, etc...
    def history(self, assets=None, name="PX_LAST"):
        return self.assets.frame(assets, name)

    @property
    def reference(self):
        return self.symbols.frame

    def asset(self, name):
        return Asset(name=name, data=pd.DataFrame(self.assets[name]), **self.symbols[name].to_dict())

    @property
    def strategies(self):
        p = Portfolios()
        for name, portfolio in self.portfolios.items():
            p[name] = portfolio
        return p

    def drop(self):
        self.portfolios.drop()
        self.symbols.drop()
        self.assets.drop()

        assert self.portfolios.empty
        assert self.symbols.empty
        assert self.assets.empty
