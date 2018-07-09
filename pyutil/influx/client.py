import os
from time import time

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
    def influxclient(self):
        return super(DataFrameClient, self)

    @property
    def databases(self):
        """ get set of databases (names) """
        return set([a["name"] for a in self.get_list_database()])

    @property
    def measurements(self):
        """ get set of measurements for a given database """
        return set([a["name"] for a in self.get_list_measurements()])

    def tags(self, measurement, key, conditions=None):
        """
        Values for a key

        :param measurement:
        :param key:
        :return:
        """
        query = 'SHOW TAG VALUES FROM {m} WITH KEY="{key}"'.format(m=measurement, key=key)

        if conditions:
            query += " WHERE {c}".format(
                c=" AND ".join([""""{tag}"::tag='{value}'""".format(tag=c[0], value=c[1]) for c in conditions]))


        return set([a["value"] for a in self.query(query).get_points()])

    def keys(self, measurement):
        """
        All keys used within a measurement...

        :param measurement:
        :return:
        """
        c = self.query('SHOW TAG KEYS FROM "{m}"'.format(m=measurement))
        return [x["tagKey"] for x in c.get_points()]

    def __cond(self, conditions=None):
        return " AND ".join([""""{tag}"::tag='{value}'""".format(tag=c[0], value=c[1]) for c in conditions])

    def __query(self, field, measurement, tags=None, conditions=None):
        query = """SELECT {f}::field""".format(f=field)

        if tags:
            query += ", {t}".format(t=", ".join(['"{t}"::tag'.format(t=t) for t in tags]))

        query += " FROM {m}".format(m=measurement)

        if conditions:
            query += " WHERE {c}".format(c=" AND ".join([""""{tag}"::tag='{value}'""".format(tag=c[0], value=c[1]) for c in conditions]))

        return query

    def frame(self, field, tags, measurement, conditions=None):
        query = self.__query(field=field, tags=tags, measurement=measurement, conditions=conditions)
        result = self.query(query)

        if measurement in result:
            x = result[measurement].tz_localize(None)
            return x.set_index(keys=tags, append=True).unstack(level=-1)[field]
        else:
            return pd.DataFrame({})

    def series(self, field, measurement, conditions=None):
        """ test empty !!!! """
        query = self.__query(field=field, measurement=measurement, conditions=conditions)
        result = self.query(query)

        if measurement in result:
            return result[measurement][field].tz_localize(None)
        else:
            return pd.Series({})

    def series_upsert(self, ts, tags, field, measurement):
        if len(ts) > 0:
            json_body = [{'measurement': measurement,'time': t, 'fields': {field: float(x)}} for t,x in ts.items()]
            self.influxclient.write_points(json_body, time_precision="s", tags=tags, batch_size=10000)

    def frame_upsert(self, frame, tags, field, measurement):
        frame = frame.stack()
        print(frame)
        frame.index.names = ["Time", "Asset"]

        frame = frame.reset_index()
        frame = frame.set_index("Time")
        print(frame)

        self.write_points(dataframe=frame, time_precision="s",tags=tags, tag_columns=["Asset"], measurement=measurement)
