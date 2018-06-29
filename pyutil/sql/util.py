import pandas as pd
from io import BytesIO


def to_pandas(x):
    if x:
        return pd.read_json(BytesIO(x).read().decode(), typ="series")
    else:
        return pd.Series({})


def from_pandas(x):
    return x.to_json().encode()


def parse(x, type="string"):

    __p = {"string": lambda x: x,
           "float": lambda x: float(x),
           "integer": lambda x: int(float(x)),
           "percentage": lambda x: float(x),
           "date": lambda x: pd.Timestamp(1e6 * int(x))}

    if not x:
        return None
    return __p[type](x)


def reference(frame):
    if frame.empty:
        return pd.DataFrame(index=frame.index, columns=["value"])

    frame = frame.dropna(axis=1, how="all")

    frame = frame[["content", "result"]].unstack(level=-1).swaplevel(axis=1)

    d = dict()
    for p in set(frame.columns.get_level_values(level=0)):
        d[p] = frame[p].apply(lambda x: parse(x[0],x[1]), axis=1)

    return pd.DataFrame(d)



