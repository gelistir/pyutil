import warnings

import pandas as pd


def update_portfolio(archive, portfolio, name, n=1):
    """
    Update a portfolio in an archive
    :param archive: archive
    :param result: result, as defined in Loop.py
    :param n: number for overwrite, e.g. if there is already a portfolio with this name...
    :return:
    """

    if name in archive.portfolios.keys():
        assert n > 0, "n has to be positive. It is {0}".format(n)
        # note that tail wouldn't work as they may leave big gaps in the portfolios... assume you don't know when
        # there was the last trading day of the portfolio in the database
        last_valid = archive.portfolios[name].index[-n]
        portfolio = portfolio.truncate(before=last_valid + pd.DateOffset(seconds=1))
    else:
        warnings.warn("The portfolio {0} is unknown in the database".format(name))
        portfolio = portfolio

    return archive.portfolios.update(key=name, portfolio=portfolio)
