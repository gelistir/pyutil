# import pandas as pd
#
#
# # todo: test this
# def reference(products):
#     x = pd.DataFrame({product: pd.Series(product.reference) for product in products}).transpose()
#     x.index.name = "Product"
#     return x
#
#
# def history(products, field="PX_LAST"):
#     x = pd.DataFrame({product: product.timeseries[field] for product in products})
#     x.index.name = "Date"
#     # delete empty columns
#     return x.dropna(axis=1, how="all")
