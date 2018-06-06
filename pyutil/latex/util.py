import pandas as pd

import numpy as np

def trend(series, width=20):
    t = r'''\begin{sparkline}{%(width)s} \sparkrectangle 0.0 1.0 \spark %(series)s / \end{sparkline}'''
    x = "  ".join([str(a) + " " + str(b) + "   " for a, b in zip(np.linspace(0, 1.0, num=series.size), series.values)])
    return t % {'series': x, 'width': str(width)}

def percentage(x):
    return "{0:.2f}\%".format(x*100)

if __name__ == '__main__':
    x = pd.Series(data=[0.0, 0.4, 0.8, 1.0])
    print(trend(x))

