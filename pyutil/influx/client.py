from influxdb import DataFrameClient, SeriesHelper

class Client(DataFrameClient):
    def __init__(self, host, port=8086):
        super().__init__(host, port)

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

    def tags(self, measurement, key):
        """
        Values for a key

        :param measurement:
        :param key:
        :return:
        """
        x = self.query('SHOW TAG VALUES FROM {m} WITH KEY="{key}"'.format(m=measurement, key=key))
        return set([a["value"] for a in x.get_points()])

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

                # Defines the number of data points to store prior to writing
                # on the wire.
                #bulk_size = bulk_size

                # autocommit must be set to True when using bulk_size
                #autocommit = autocommit

        MySeriesHelper.Meta.fields = fields
        MySeriesHelper.Meta.tags = tags
        MySeriesHelper.Meta.client = self.influxclient
        MySeriesHelper.Meta.series_name = series_name
        MySeriesHelper.Meta.bulk_size = bulk_size
        MySeriesHelper.Meta.autocommit = autocommit

        return MySeriesHelper
