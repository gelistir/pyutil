from .pandasdocument import PandasDocument
from ...portfolio.portfolio import Portfolio


class Prospect(PandasDocument):
    def __init__(self, *args, **values):
        super().__init__(*args, **values)

        assert hasattr(self, "prices")
        assert hasattr(self, "position")

    def portfolio(self, cash=0):
        return Portfolio.fromPosition(prices=self.prices.ffill()[self.position.index], position=self.position, cash=cash)
