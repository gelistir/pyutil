# import collections
#
# import pandas as pd
#
#
# def frame2dict(x):
#     y = x.copy()
#     y.index = [a.strftime("%Y%m%d") for a in y.index]
#     return {k: v.dropna().to_dict() for k, v in y.items()}
#
#
# def flatten(d, parent_key=None, sep='.'):
#     """ flatten dictonaries of dictionaries (of dictionaries of dict... you get it)"""
#     items = []
#     for k, v in d.items():
#         new_key = parent_key + sep + k if parent_key else k
#         if isinstance(v, collections.MutableMapping):
#             items.extend(flatten(v, new_key, sep=sep).items())
#         else:
#             items.append((new_key, v))
#     return dict(items)
#
#
# def dict2frame(dictionary):
#     x = pd.DataFrame({name: pd.Series(data) for name, data in dictionary.items()})
#     x.index = [pd.Timestamp(a) for a in x.index]
#     return x
#
