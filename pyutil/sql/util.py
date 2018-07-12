import pandas as pd

def parse(rows, index):
    g = pd.DataFrame(rows)
    if g.empty:
        return pd.Series()
    else:
        g = g.set_index(index)
        return g[["content", "result"]].apply(lambda x: x[1].parse(x[0]), axis=1)