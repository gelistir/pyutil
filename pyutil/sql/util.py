import pandas as pd


def parse(x, typ="string"):
    __p = {"string": lambda x: x,
           "float": lambda x: float(x),
           "integer": lambda x: int(float(x)),
           "percentage": lambda x: float(x),
           "date": lambda x: pd.Timestamp(1e6 * int(x))}

    if not x:
        return None

    return __p[typ](x)


def reference(frame):
    if frame.empty:
        return pd.DataFrame(index=frame.index, columns=["value"])

    frame = frame.dropna(axis=1, how="all")
    return frame[["content", "result"]].apply(lambda x: parse(x[0], x[1]), axis=1).unstack(level=-1)



