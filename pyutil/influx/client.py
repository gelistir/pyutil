import datetime
import os
from datetime import date

import pandas as pd
from influxdb import DataFrameClient


class Client(DataFrameClient):
    def __init__(self, host=None, port=8086, database=None):
        host = host or os.environ["INFLUXDB_HOST"]
        super().__init__(host, port)
        if database:
            self.create_database(dbname=database)
            self.switch_database(database=database)

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

    def read_series(self, field, measurement, tags=None, conditions=None, unstack=False):
        try:
            a = self.__read_frame(measurement=measurement, tags=tags, conditions=conditions)
            a = a[field]
            if unstack:
                return a.unstack()
            else:
                return a

        except KeyError:
            return pd.Series({})

    def write_series(self, ts, field, measurement, tags=None, batch_size=5000, time_precision="s"):
        if len(ts) > 0:
            # convert from date to datetime if needed...
            if isinstance(ts.index[0], datetime.date):
                ts.index = [pd.Timestamp(a) for a in ts.index]

            self.write_points(dataframe=ts.to_frame(name=field), measurement=measurement, tags=tags, field_columns=[field],
                              batch_size=batch_size, time_precision=time_precision)

    def __read_frame(self, measurement, field="*", tags=None, conditions=None):
        q = "SELECT {f}::field {t} from {m}{co}""".format(f=field, t=self.__tags(tags), m=measurement, co=self.__cond(conditions))
        try:
            x = self.query(q)[measurement].tz_localize(None)
            if tags:
                x = x.set_index(tags, append=True)
            return x
        except:
            return pd.DataFrame({})

    # No, you can't update an entire frame for a single symbol!
    def last(self, measurement, field, conditions=None):
        try:
            query = """SELECT LAST({f}) FROM {m}{c}""".format(f=field, m=measurement, c=self.__cond(conditions))
            return self.query(query)[measurement].index[0].tz_localize(None)
        except KeyError:
            return None
