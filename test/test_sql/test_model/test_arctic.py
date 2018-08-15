import pandas as pd
import time

from arctic import Arctic, CHUNK_STORE
from arctic.exceptions import NoDataFoundException

# Connect to Local MONGODB
from test.config import test_portfolio

lib = "NAV12"
store = Arctic('quantsrv')

# Create the library - defaults to VersionStore
store.initialize_library(lib) #, lib_type=CHUNK_STORE)

# Access the library
library = store[lib]
print(dir(library))
print(version)

# has to be a frame!
nav = test_portfolio().nav.to_frame(name="nav")
nav.index.name = "date"

x = pd.Series(index=[pd.Timestamp("2010-04-21"), pd.Timestamp("2011-03-22")], data=[2.0, 3.0])
#x.index.name = "date"

library.write('XXX', x, metadata={"last": None})
print(library.read('XXX'))
assert False


library.update('AAPL', nav, metadata={"source": "private", "time": time.time(), "last": nav.last_valid_index()})
library.update('AAPL', 1000*nav.tail(6).head(5), metadata={"source": "private", "time": time.time(), "last": nav.last_valid_index()})

#print(library)

#assert False

item = library.read('AAPL')
print(library.read_metadata('AAPL'))
#print(item.metadata["last"])
print(item.tail(10))
assert False

#try:
#    xxx = library.read('AAPL2').data.last_valid_index().date()
#except NoDataFoundException:
#    xxx = pd.Timestamp("2002-01-01").date()

print(xxx)
assert False

xxx = item.data
assert False

metadata = item.metadata

#print(xxx)
#print(metadata)
print(time.time() - t1)
print(len(xxx))
print(xxx.tail(10))