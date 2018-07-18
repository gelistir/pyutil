import os

os.environ["influxdb_host"] = "test-influxdb"
os.environ["influxdb_db"] = "test"

from pyutil.sql.interfaces.products import ProductInterface

def init_influxdb():
    ProductInterface.client.recreate(dbname=os.environ["influxdb_db"])
