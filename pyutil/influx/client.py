import datetime
import logging
import os
from contextlib import ExitStack

import pandas as pd
from influxdb import DataFrameClient


class Client(DataFrameClient, ExitStack):
    def __init__(self, host=None, port=8086, database=None, logger=None):

        host = host or os.environ["influxdb_host"]
        self.__database = database or os.environ["influxdb_db"]

        super().__init__(host, port)

        self.create_database(dbname=self.database)
        self.switch_database(database=self.database)
        self.__logger = logger or logging.getLogger(__name__)

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()

    def recreate(self, dbname):
        self.drop_database(dbname=dbname)
        self.create_database(dbname=dbname)
        self.switch_database(database=dbname)
        self.__database = dbname

    @property
    def database(self):
        return self.__database

    @property
    def host(self):
        return super()._host

    @property
    def port(self):
        return super()._port

    def __repr__(self):
        return "InfluxClient at {host} on port {port}".format(host=self.host, port=self.port)

    @property
    def databases(self):
        """ get set of databases (names) """
        return set([a["name"] for a in self.get_list_database()])

    @property
    def measurements(self):
        """ get set of measurements for a given database """
        return set([a["name"] for a in self.get_list_measurements()])

    @staticmethod
    def __cond(conditions=None):
        if conditions:
            return " WHERE {c}".format(c=" AND ".join(
                    [""""{tag}"::tag='{value}'""".format(tag=key, value=value) for key, value in conditions.items()]))
        else:
            return ""

    @staticmethod
    def __tags(tags=None):
        if tags:
            return ", {t}".format(t=", ".join(['"{t}"::tag'.format(t=t) for t in tags]))
        else:
            return ""

    def read_series(self, field, measurement, tags=None, conditions=None):
        try:
            return self.__read_frame(measurement=measurement, field=field, tags=tags, conditions=conditions)[field].dropna()
        except KeyError:
            return pd.Series({})

    def read_frame(self, field, measurement, tags=None, conditions=None):
        #try:
        a = self.__read_frame(measurement=measurement, field=field, tags=tags, conditions=conditions)
        return a[field].unstack(level=-2)

        #except:
        #    return pd.DataFrame({})

    def write_series(self, ts, field, measurement, tags=None, batch_size=5000, time_precision="s"):
        if len(ts) > 0:
            # convert from date to datetime if needed...
            if isinstance(ts.index[0], datetime.date):
                ts.index = [pd.Timestamp(a) for a in ts.index]

            self.write_points(dataframe=ts.to_frame(name=field), measurement=measurement, tags=tags, field_columns=[field],
                              batch_size=batch_size, time_precision=time_precision)

    def __read_frame(self, measurement, field="*", tags=None, conditions=None):
        q = "SELECT {f}::field {t} from {m}{co}""".format(f=field, t=self.__tags(tags), m=measurement, co=self.__cond(conditions))
        self.__logger.debug("Query {q}".format(q=q))

        #try:
        x = self.query(q)[measurement].tz_localize(None)
        self.__logger.debug("Head {h}".format(h=x.head(10)))

        if tags:
            assert isinstance(tags, list)
            # it would be nice if date is becoming the last index...
            x["time"] = x.index
            tags.append("time")
            x = x.set_index(keys=tags)
        return x
        #except:
        #    return pd.DataFrame({})

    # No, you can't update an entire frame for a single symbol!
    def last(self, measurement, field, conditions=None):
        try:
            query = """SELECT LAST({f}) FROM {m}{c}""".format(f=field, m=measurement, c=self.__cond(conditions))
            return self.query(query)[measurement].index[0].tz_localize(None)
        except KeyError:
            return None

