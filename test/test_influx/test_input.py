import pandas as pd
from unittest import TestCase
from pyutil.influx.client import Client

# InfluxDB connections settings
host = 'test-influxdb'
port = 8086
dbname = 'mydb'


class TestInput(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client(host='test-influxdb')
        for db in cls.client.databases:
            cls.client.drop_database(dbname=db)

        cls.client.create_database(dbname)
        cls.client.switch_database(dbname)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def test_a(self):
        hhh = self.client.helper(tags=['name'], fields=['nav','leverage'], series_name='portfolio')


        hhh(name='east-1', nav=159, leverage=10, time=pd.Timestamp("2010-01-01"))
        hhh(name='east-1', nav=158, leverage=20, time=pd.Timestamp("2010-01-02"))
        hhh(name='east-1', nav=157, leverage=30, time=pd.Timestamp("2010-01-03"))
        hhh(name='east-1', nav=156, leverage=40, time=pd.Timestamp("2010-01-04"))
        hhh(name='east-1', nav=155, leverage=50, time=pd.Timestamp("2010-01-05"))
        hhh(name='west-1', nav=156, leverage=40, time=pd.Timestamp("2010-01-01"))
        hhh(name='west-1', nav=155, leverage=50, time=pd.Timestamp("2010-01-02"))

        # To manually submit data points which are not yet written, call commit:
        hhh.commit()

        print(self.client.databases)
        print(self.client.measurements)
        print(self.client.query("""SELECT "nav"::field, "name"::tag FROM portfolio""")["portfolio"].set_index(keys=["name"], append=True).unstack(level=-1)["nav"])


