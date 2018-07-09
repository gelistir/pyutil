import os

import pandas as pd

from influxdb import DataFrameClient, SeriesHelper

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

    def helper(self, tags, fields, series_name, bulk_size=5, autocommit=True):
        class MySeriesHelper(SeriesHelper):
            """Instantiate SeriesHelper to write points to the backend."""

            class Meta:
                """Meta class stores time series helper configuration."""
                pass

        MySeriesHelper.Meta.fields = fields
        MySeriesHelper.Meta.tags = tags
        MySeriesHelper.Meta.client = self.influxclient
        MySeriesHelper.Meta.series_name = series_name
        MySeriesHelper.Meta.bulk_size = bulk_size
        MySeriesHelper.Meta.autocommit = autocommit

        return MySeriesHelper

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

    def series_upsert(self, ts, tags, field, series_name, bulk_size=2000, autocommit=True):
        if len(ts) > 0:
            helper = self.helper(tags=list(tags.keys()), fields=[field], series_name=series_name, autocommit=autocommit, bulk_size=bulk_size)
            for t, x in ts.items():
                helper(**{**{field: float(x), "time": pd.Timestamp(t).replace(second=0, microsecond=0, nanosecond=0)}, **tags})

            helper.commit()
