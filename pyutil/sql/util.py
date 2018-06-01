import pandas as pd
from io import BytesIO


def to_pandas(x):
    if x:
        return pd.read_json(BytesIO(x).read().decode(), typ="series")
    else:
        return pd.Series({})

def from_pandas(x):
    return x.to_json().encode()

__p = {"string": lambda x: x,
       "float": lambda x: float(x),
       "integer": lambda x: int(float(x)),
       "percentage": lambda x: float(x),
       "date": lambda x: pd.Timestamp(1e6*int(x))}

def parse(x, type="string"):
    return __p[type](x)




