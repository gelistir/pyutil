import pandas as pd


def merge(new, old=None):
    # very smart merging here, new and old merge
    x = pd.concat((new, old))
    return x.groupby(x.index).first().sort_index()