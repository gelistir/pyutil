# import pandas as pd
# from unittest import TestCase
# from pyutil.influx.client import Client
#
# import pandas.util.testing as pdt
#
# # InfluxDB connections settings
# host = 'test-influxdb'
# port = 8086
# dbname = 'mydb'
#
#
# class TestInput(TestCase):
#     @classmethod
#     def setUpClass(cls):
#         cls.client = Client(host='test-influxdb', database="dbname")
#
#     @classmethod
#     def tearDownClass(cls):
#         cls.client.close()
#
#     def test_a(self):
#         hhh = self.client.helper(tags=['name'], fields=['nav','leverage'], series_name='portfolio')
#
#
#         hhh(name='east-1', nav=159, leverage=10, time=pd.Timestamp("2010-01-01"))
#         hhh(name='east-1', nav=158, leverage=20, time=pd.Timestamp("2010-01-02"))
#         hhh(name='east-1', nav=157, leverage=30, time=pd.Timestamp("2010-01-03"))
#         hhh(name='east-1', nav=156, leverage=40, time=pd.Timestamp("2010-01-04"))
#         hhh(name='east-1', nav=155, leverage=50, time=pd.Timestamp("2010-01-05"))
#         hhh(name='west-1', nav=156, leverage=40, time=pd.Timestamp("2010-01-01"))
#         hhh(name='west-1', nav=155, leverage=50, time=pd.Timestamp("2010-01-02"))
#
#         # To manually submit data points which are not yet written, call commit:
#         hhh.commit()
#
#         print(self.client.databases)
#         print(self.client.measurements)
#         print(self.client.frame(field="nav", tags=["name"], measurement="portfolio"))
#         print(self.client.query("""SELECT "nav"::field, "name"::tag FROM portfolio""")["portfolio"].set_index(keys=["name"], append=True).unstack(level=-1)["nav"])
#         print(self.client.series(field="nav", measurement="portfolio", conditions=[("name","east-1")]))
#
#
#         pdt.assert_series_equal(self.client.series(field="nav", measurement="portfolio", conditions=[("name","east-2")]), pd.Series({}))
#
#         pdt.assert_frame_equal(self.client.frame(field="nav2", tags=["name"], measurement="portfolio"), pd.DataFrame({}))
