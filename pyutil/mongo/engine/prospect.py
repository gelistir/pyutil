#import pandas as pd
import pandas as pd

from .pandasdocument import PandasDocument
from ...portfolio.portfolio import Portfolio


class Prospect(PandasDocument):
    def portfolio(self, cash=0):
        return Portfolio.fromPosition(prices=self.prices.ffill()[self.position.index], position=self.position, cash=cash)
