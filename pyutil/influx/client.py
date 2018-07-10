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

    def tag_values(self, measurement, key, conditions=None):
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

    def tag_keys(self, measurement):
        """
        All keys used within a measurement...

        :param measurement:
        :return:
        """
        c = self.query('SHOW TAG KEYS FROM "{m}"'.format(m=measurement))
        return [x["tagKey"] for x in c.get_points()]

    def __cond(self, conditions=None):
        return " AND ".join([""""{tag}"::tag='{value}'""".format(tag=c[0], value=c[1]) for c in conditions])

    def read_series(self, field, measurement, conditions=None):
        """ test empty !!!! """
        try:
            a = self.read_frame(measurement=measurement, conditions=conditions)[field]
            #a.name = None
            return a
        except KeyError:
            return pd.Series({})

    def write_series(self, ts, field, measurement, tags=None):
        if len(ts) > 0:
            self.write_frame(ts.to_frame(name=field.replace(" ", "_")), measurement=measurement, tags=tags)

    def write_portfolio(self, portfolio, name, batch_size=500, time_precision=None):
        self.write_frame(frame=portfolio.prices, measurement="prices", tags={"name": name}, batch_size=batch_size, time_precision=time_precision)
        self.write_frame(frame=portfolio.weights, measurement="weights", tags={"name": name}, batch_size=batch_size, time_precision=time_precision)

    def read_portfolio(self, name):
        p = self.read_frame(measurement="prices", conditions=[("name", name)])
        w = self.read_frame(measurement="weights", conditions=[("name", name)])
        return p,w

    def read_frame(self, measurement, tags=None, conditions=None):
        query = "SELECT *::field"

        if tags:
            query += ", {t}".format(t=", ".join(['"{t}"::tag'.format(t=t) for t in tags]))

        query += " from {measurement}""".format(measurement=measurement)

        if conditions:
            query += " WHERE {c}".format(c=" AND ".join([""""{tag}"::tag='{value}'""".format(tag=c[0], value=c[1]) for c in conditions]))

        x = self.query(query)

        if measurement in x.keys():
            x = x[measurement].tz_localize(None)

            if tags:
                return x.set_index(tags, append=True)

            return x.rename(columns=lambda x: x.replace("_", " "))
        else:
            return pd.DataFrame({})

    def write_frame(self, frame, measurement, tags=None, batch_size=500, time_precision=None):
        a = frame.rename(columns=lambda x: x.replace(" ", "_"))
        self.write_points(dataframe=a.applymap(float), measurement=measurement, tags=tags, field_columns=list(a.keys()), batch_size=batch_size, time_precision=time_precision)
