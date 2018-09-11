import datetime
import logging
import os
from contextlib import ExitStack

import pandas as pd
from influxdb import DataFrameClient


class Client(ExitStack):
    def __init__(self, host=None, port=None, database=None, logger=None, username=None, password=None):

        host = host or os.environ["influxdb_host"]
        port = port or os.environ["influxdb_port"]
        self.__database = database or os.environ["influxdb_db"]
        username = username or os.environ["influxdb_username"]
        password = password or os.environ["influxdb_password"]

        self.__client = DataFrameClient(host=host, port=port, username=username, password=password)

        # only try to create the database if it doesn't exist! You would need admin rights here...
        if not self.database in self.databases:
            self.__client.create_database(dbname=self.database)

        self.__client.switch_database(database=self.database)
        self.__logger = logger or logging.getLogger(__name__)

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()

    def recreate(self, dbname):
        self.__client.drop_database(dbname=dbname)
        self.__client.create_database(dbname=dbname)
        self.__client.switch_database(database=dbname)
        self.__database = dbname

    def close(self):
        self.__client.close()

    def drop_database(self, dbname):
        self.__client.drop_database(dbname=dbname)

    @property
    def database(self):
        return self.__database

    @property
    def host(self):
        return self.__client._host

    @property
    def port(self):
        return self.__client._port

    def __repr__(self):
        return "InfluxClient at {host} on port {port}".format(host=self.host, port=self.port)

    @property
    def databases(self):
        """ get set of databases (names) """
        return set([a["name"] for a in self.__client.get_list_database()])

    @property
    def measurements(self):
        """ get set of measurements for a given database """
        return set([a["name"] for a in self.__client.get_list_measurements()])

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

    def read(self, field, measurement, tags=None, conditions=None):
        # always return a series, tags show up in the Multiindex!
        try:
            return self.__read_frame(measurement=measurement, field=field, tags=tags, conditions=conditions)[field].dropna()
        except KeyError:
            return pd.Series({})

    def write(self, frame, measurement, field_columns=None, tag_columns=None, tags=None, batch_size=5000, time_precision="s"):
        if len(frame.index) > 0:
            if isinstance(frame.index[0], datetime.date):
                frame.index = [pd.Timestamp(a) for a in frame.index]

            assert isinstance(frame, pd.DataFrame)

            self.__client.write_points(frame, measurement=measurement, tag_columns=tag_columns, tags=tags,
                                       field_columns=field_columns, batch_size=batch_size, time_precision=time_precision)

    def __read_frame(self, measurement, field="*", tags=None, conditions=None):
        q = "SELECT {f}::field {t} from {m}{co}""".format(f=field, t=self.__tags(tags), m=measurement, co=self.__cond(conditions))

        x = self.__client.query(q)[measurement].tz_localize(None)
        x.index.name = "time"

        if tags:
            # append the tags to the index, time remains the first column in the new Multiindex
            assert isinstance(tags, list)
            x = x.set_index(keys=tags, append=True)

        return x

    # No, you can't update an entire frame for a single symbol!
    def last(self, measurement, field, conditions=None):
        try:
            query = """SELECT LAST({f}) FROM {m}{c}""".format(f=field, m=measurement, c=self.__cond(conditions))
            return self.__client.query(query)[measurement].index[0].tz_localize(None)
        except KeyError:
            return None
