import pandas as pd

from pyutil.portfolio.portfolio import Portfolio

prices = pd.DataFrame(index=[1,2,3], columns=["A","B"], data=[[1,1],[1.1,1.2],[1.1,1.4]])
print(prices)

position = pd.Series(index=["A","B"], data=[100, 200])
print(position)


def series2frame(index, series):
    f = pd.DataFrame(index=index, columns=series.index)
    for key in series.index:
        f[key] = series[key]

    return f


f = series2frame(index=prices.index, series=position)
print(f)

frame = f.multiply(prices)
cash = 5

value = frame.sum(axis=1) + cash
print(value)

weight = frame.apply(lambda x: x/value)
print(weight)
print(weight.sum(axis=1))

p=Portfolio(prices=prices, weights=weight)
print(p.position)
print(p.cash + weight.sum(axis=1))


# Bring on a client:
# a) Define prices
# b) Thomas holds






