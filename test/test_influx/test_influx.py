import pandas as pd
from unittest import TestCase

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
    },
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
        cls.client = Client(host='test-influxdb', database="testexample")
        cls.client.influxclient.write_points(json_body)

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

        print(self.client.frame(field="dura", tags=["user"], measurement="brushEvents"))
        print(self.client.series(field="dura", conditions=[("user", "Carol")], measurement="brushEvents"))

    def test_find_tags(self):
        print(self.client.tags(measurement="brushEvents",key="user"))
        self.assertSetEqual(self.client.tags(measurement="brushEvents",key="user"), {"Peter","Carol"})

    def test_write(self):
        x = pd.DatetimeIndex(start=pd.Timestamp("2010-01-01"), periods=10000, freq="D")
        y = pd.DataFrame(index=x, data=pd.np.random.randn(10000, 10), columns=["A","B","C","D","E","F","G","H","I","J"])

        for key, data in y.items():
            self.client.series_upsert(ts=data, tags={"Interpret": "Peter Maffay", "name": key}, field="heartbeat", measurement="song")

        #self.client.frame_upsert(y, tags={"Interpret": "Peter Maffay"}, field="heartbeat", measurement="song2")




