import os
import pandas as pd

from jupyteraux.bloomberg import prices
from pyutil.portfolio.portfolio import Portfolio


class Client(object):
    @staticmethod
    def __create_folder(name):
        # create a folder for the client
        try:
            os.makedirs(os.path.join("clients", name))
        except FileExistsError:
            pass

        return os.path.join("clients", name)

    def __init__(self, name, file="position.csv"):
        self.__name = name
        file = os.path.join(Client.__create_folder(name=name), file)
        assert os.path.exists(file)

        self.__position = pd.read_csv(file, index_col=0, header=0)

        assert "Name" in self.position.keys()
        assert "Position" in self.position.keys()

    @property
    def folder(self):
        return os.path.join("clients", name)

    @property
    def name(self):
        return self.__name

    @property
    def position(self):
        return self.__position

    def prices(self, t0=pd.Timestamp("2016-01-01")):
        h = prices(symbols=self.position.index, t0=t0)
        return h.rename(columns=self.position["Name"])

    def portfolio(self, prices, cash=0):
        return Portfolio.fromPosition(prices=prices, position=self.position["Position"], cash=cash).rename(position["Name"])
