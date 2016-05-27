import numpy as np

def __f(x, n):
    a = np.floor(np.log10(np.abs(x))) - n + 1
    if a < 0:
        return np.floor(x)
    else:
        b = np.power(10, a)
        return np.floor(x / b) * b


xround = np.vectorize(__f, excluded="n")


def buy_or_sell(x):
    return "Buy" if x >= 0 else "Sell"

