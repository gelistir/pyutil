import collections

import pandas as pd
import logging
import warnings

from pymongo import MongoClient
from pymongo.database import Database

from pyutil.mongo.asset import Asset
from pyutil.mongo.assets import Assets
from pyutil.mongo.portfolios import Portfolios
from ..portfolio.portfolio import Portfolio


import uuid


def _is_pandas(x):
    return isinstance(x, pd.DataFrame) or isinstance(x, pd.Series)

def _f(x):
    # change index of a frame or series
    assert _is_pandas(x)
    f = x.copy()
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


def _to_dict(x):
    assert _is_pandas(x)
    if isinstance(x, pd.DataFrame):
        return {k: v.dropna().to_dict() for k,v in x.items()}
    else:
        # series to dictionary
        return x.dropna().to_dict()


def _mongo(x):
    y = x.copy()
    try:
        # This is better than calling x.index.strftime directly as it also works for dates
        y.index = [a.strftime("%Y%m%d") for a in y.index]
    except AttributeError:
        warnings.warn("You are trying to convert the indizes of Pandas object into str. "
                      "They are currently {0} and of type {1}".format(x.index[0], type(x.index[0])))
        pass

    return _to_dict(x=y)


class MongoArchive(object):
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

        def __update(self, asset, ts, name="PX_LAST"):
            """Update time series data for an asset"""
            self.logger.debug("Asset: {0}, Name of ts: {1}, Len of ts: {2}".format(asset, name, len(ts.dropna().index)))

            # ts empty? Get out here...
            if not ts.empty:
                m = {"_id": asset}
                # look for the asset in database
                if self.db.find_one(m):
                    # asset already in database, kind of slow to write
                    self.logger.debug({name: _mongo(ts)})
                    self.db.update(m, {"$set": _flatten({name: _mongo(ts)})}, upsert=True)
                else:
                    # asset not in the database
                    self.db.insert_one({"_id": asset, name: _mongo(ts)})

        def update(self, asset):
            """Update time series data for an asset"""
            #for name, asset in assets.items():
            for series in asset.series_names():
                self.__update(asset=asset.name, ts=asset[series], name=series)

        def __getitem__(self, item):
            d = super().__getitem__(item)
            if d:
                return Asset(name=item, data=_f(pd.DataFrame({key: pd.Series(values) for key, values in d.items()})))
            else:
                return None

    class __Symbols(__DB):
        def __init__(self, db, logger=None):
            super().__init__(db=db, logger=logger)

        def update(self, asset):
            # this is slow if we update an empty database
            if asset.reference.empty:
                pass
            else:
                self.db.update({"_id": asset.name}, {"$set": _flatten(asset.reference)}, upsert=True)

        def __getitem__(self, item):
            return pd.Series(super().__getitem__(item))

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

    def __init__(self, db=str(uuid.uuid4()), host="mongo", port=27017, user=None, password=None, logger=None):
        """
        Mongo Archive for data
        :param db: Name of the Mongo Database, e.g. "production"
        :param host: Host
        :param port: Port
        :param logger: Logger
        """
        self.logger = logger or logging.getLogger(__name__)
        client = MongoClient(host=host, port=port)
        self.logger.info("Client: {0}".format(client))

        if user and password:
            self.logger.info("User: {0}".format(user))
            client.admin.authenticate(name=user, password=password)

        self.__db = Database(client, db)
        self.logger.info("Archive (read-access) at {0}".format(self.__db))

        # the database will have (at least) 3 collections.
        self.portfolios = self.__Portfolios(self.__db.strategy, logger=self.logger)
        self.__symbols = self.__Symbols(db=self.__db.symbols, logger=self.logger)
        self.__assets = self.__Assets(db=self.__db.assets, logger=self.logger)

    def __repr__(self):
        return "Reader for {0}".format(self.__db)

    def asset(self, name):
        return Asset(name=name, data=self.__assets[name].data, **self.__symbols[name].to_dict())

    @property
    def strategies(self):
        p = Portfolios()
        for name, portfolio in self.portfolios.items():
            p[name] = portfolio
        return p

    def update_asset(self, asset):
        assert isinstance(asset, Asset)
        self.__symbols.update(asset)
        self.__assets.update(asset)

    def names(self):
        return self.__symbols.keys()

    def assets(self, names=None):
        """
        Construct assets based on a list of names
        """
        if names is not None:
            return Assets([self.asset(name) for name in names])
        else:
            return Assets([self.asset(name) for name in self.names()])