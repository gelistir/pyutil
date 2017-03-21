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
            return self.db.find_one({"_id": item}, {"_id": 0})

        def items(self):
            for k in self.keys():
                yield (k, self[k])

        def __setitem__(self, key, value):
            # this is not implemented, use update routines
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

        def update_series(self, asset_name, series, series_name="PX_LAST"):
            """Update time series data for an asset"""
            self.logger.debug("Asset: {0}, Name of ts: {1}, Len of ts: {2}".format(asset_name, series_name, len(series.dropna().index)))

            # ts empty? Get out here...
            if not series.empty:
                m = {"_id": asset_name}
                # look for the asset in database
                if self.db.find_one(m):
                    # asset already in database, kind of slow to write
                    self.db.update(m, {"$set": _flatten({series_name: _mongo(series)})}, upsert=True)
                else:
                    # asset not in the database
                    self.db.insert_one({"_id": asset_name, series_name: _mongo(series)})

        def __getitem__(self, item):
            """ return a DataFrame! """
            d = super().__getitem__(item)
            if d is not None:
                return _f(pd.DataFrame({key: pd.Series(values) for key, values in d.items()})).sort_index(axis=1).dropna(how="all", axis=0)
            else:
                raise KeyError("The asset {0} is not known in the time series database".format(item))

        def update_all(self, frame, name="PX_LAST"):
            for key, series in frame.iteritems():
                self.update_series(key, series=series.dropna(), series_name=name)


    class __Symbols(__DB):d
        def __init__(self, db, logger=None):
            super().__init__(db=db, logger=logger)

        def update(self, asset_name, ref_series):
            # this is slow if we update an empty database
            if ref_series.empty:
                pass
            else:
                self.db.update({"_id": asset_name}, {"$set": _flatten(ref_series)}, upsert=True)

        def __getitem__(self, item):
            return pd.Series(super().__getitem__(item))

        def update_all(self, frame):
            for key in frame.index:
                self.update(asset_name=key, ref_series=frame.ix[key])


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
        self.symbols = self.__Symbols(db=self.__db.symbols, logger=self.logger)
        self.time_series = self.__Assets(db=self.__db.assets, logger=self.logger)

    def __repr__(self):
        return "Reader for {0}".format(self.__db)

    @property
    def reader(self):
        """
        Exposes a function pointer. This is given to the strategies. They can then only extract the information needed.
        :return:
        """
        return lambda name: Asset(name=name, data=self.time_series[name], **self.symbols[name].to_dict())

    @property
    def strategies(self):
        if self.portfolios.empty:
            return None
        else:
            p = Portfolios()
            for name, portfolio in self.portfolios.items():
                p[name] = portfolio
            return p

    def assets(self, names):
        """
        Construct assets based on a list of names
        """
        return Assets([self.reader(name) for name in names])

    def history(self, name):
        asset_names = self.time_series.keys()
        if not asset_names:
            # no assets in database
            return pd.DataFrame()
        else:
            x = pd.concat({asset: self.time_series[asset] for asset in asset_names}, axis=1).swaplevel(axis=1)
            if name in x.keys():
                return x[name]
            else:
                return pd.DataFrame()

    @property
    def reference(self):
        return pd.DataFrame({key: self.symbols[key] for key in self.symbols.keys()}).transpose().sort_index(axis=1)


