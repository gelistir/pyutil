import os
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

    def __tag_values(self, measurement, key, conditions=None):
        query = 'SHOW TAG VALUES FROM {m} WITH KEY="{key}{conditions}"'.format(m=measurement, key=key, conditions=self.__cond(conditions))
        return set([a["value"] for a in self.query(query).get_points()])

    @property
    def portfolios(self):
        return self.__tag_values(measurement="prices", key="name")

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

    def write_series(self, ts, field, measurement, tags=None):
        if len(ts) > 0:
            self.__write_frame(ts.to_frame(name=field), measurement=measurement, tags=tags)

    # todo: move to portfolio class?
    def write_portfolio(self, portfolio, name, batch_size=500, time_precision=None):
        self.__write_frame(frame=portfolio.prices, measurement="prices", tags={"name": name}, batch_size=batch_size,
                           time_precision=time_precision)
        self.__write_frame(frame=portfolio.weights, measurement="weights", tags={"name": name}, batch_size=batch_size,
                           time_precision=time_precision)

    def read_portfolio(self, name):
        #p = self.read_series(field="*", measurement="prices", tags=["name"], conditions={"name": name}, unstack=True).rename(columns=lambda x: x.replace("_", " "))
        p = self.__read_frame(measurement="prices", conditions={"name": name}).rename(
            columns=lambda x: x.replace("_", " "))
        w = self.__read_frame(measurement="weights", conditions={"name": name}).rename(
            columns=lambda x: x.replace("_", " "))
        return p, w

    def __read_frame(self, measurement, field="*", tags=None, conditions=None):
        q = "SELECT {f}::field {t} from {m}{co}""".format(f=field, t=self.__tags(tags), m=measurement, co=self.__cond(conditions))
        try:
            x = self.query(q)[measurement].tz_localize(None)
            if tags:
                x = x.set_index(tags, append=True)
            return x
        except:
            return pd.DataFrame({})

    def __write_frame(self, frame, measurement, tags=None, batch_size=500, time_precision=None):
        a = frame.rename(columns=lambda x: x.replace(" ", "_"))

        self.write_points(dataframe=a.applymap(float), measurement=measurement, tags=tags,
                          field_columns=list(a.keys()), batch_size=batch_size, time_precision=time_precision)

    # No, you can't update an entire frame for a single symbol!
    def last(self, measurement, field, conditions=None):
        try:
            query = """SELECT LAST({f}) FROM {m}{c}""".format(f=field, m=measurement, c=self.__cond(conditions))
            return self.query(query)[measurement].index[0].tz_localize(None)
        except KeyError:
            return None
