import pandas as pd
from unittest import TestCase

from influxdb import InfluxDBClient, DataFrameClient
from pyutil.influx.client import Client

json_body = [
    {
        "measurement": "brushEvents",
        "tags": {
            "user": "Carol",
            #"brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
        },
        "time": "2018-03-28T8:01:00Z",
        "fields": {
            "dura": 127,
            "weight": 10
        }
    },
    {
        "measurement": "brushEvents",
        "tags": {
            "user": "Carol",
            "xxx": "Wurst"
            #"brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
        },
        "time": "2018-03-29T8:04:00Z",
        "fields": {
            "dura": 132,
            "weight": 11
        }
    },
    {
        "measurement": "brushEvents",
        "tags": {
            "user": "Carol",
            #"brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
        },
        "time": "2018-03-30T8:02:00Z",
        "fields": {
            "dura": 129,
            "weight": 10
        }
    },
    {
        "measurement": "brushEvents",
        "tags": {
            "user": "Peter",
            #"brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2g"
        },
        "time": "2018-03-30T8:02:00Z",
        "fields": {
            "dura": 229,
            "weight": 20
        }
    }
]


json_body2 = [
    {
        "measurement": "port",
        "tags": {
            "owner": "Hans",
            "asset": "Asset B",
            "custodian": "UBS"
            #"brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
        },
        "time": "2018-03-28T8:01:00Z",
        "fields": {
            "price": 120,
            "weight": 0.10
        }
    }
]

class TestInfluxDB(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client(host='test-influxdb')

        for db in cls.client.databases:
            cls.client.drop_database(db)

        cls.client.create_database('testexample')
        cls.client.switch_database('testexample')

        cls.client.influxclient.write_points(json_body)
        cls.client.influxclient.write_points(json_body2)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def test_client(self):
        databases = self.client.databases
        self.assertTrue("testexample" in databases)

    def test_measurments(self):
        measurements = self.client.measurements
        self.assertTrue("brushEvents" in measurements)
        self.assertTrue("port" in measurements)
        self.assertTrue(len(measurements), 2)

    def test_frame(self):
        print(self.client.keys(measurement="brushEvents"))
        self.assertSetEqual({"xxx", "user"}, set(self.client.keys(measurement="brushEvents")))

        a = self.client.query("SELECT * FROM brushEvents")["brushEvents"].set_index(keys=["user","xxx"], append=True)
        print(a)
        #assert False

        # this query doesn' make a lot of sense as no tag is included!
        a = self.client.query("SELECT dura FROM brushEvents")["brushEvents"]
        print(a)
        #assert False

        a = self.client.query("""SELECT dura::field, "user"::tag FROM brushEvents""")["brushEvents"].set_index(keys=["user"], append=True)
        print(a)
        print(a.unstack(level=-1)["dura"])

        a = self.client.query("""SELECT dura::field FROM brushEvents WHERE "user"='Carol'""")["brushEvents"]
        print(a["dura"])

    def test_find_tags(self):
        print(self.client.tags(measurement="brushEvents",key="user"))
        self.assertSetEqual(self.client.tags(measurement="brushEvents",key="user"), {"Peter","Carol"})
