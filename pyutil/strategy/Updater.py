import warnings


def update_portfolio(archive, result, n=5):
    """
    Update a portfolio in an archive
    :param archive: archive
    :param result: result, as defined in Loop.py
    :param n: number for overwrite, e.g. if there is already a portfolio with this name...
    :return:
    """
    #for key, item in results.items():
    if result.name in archive.portfolios.keys():
        portfolio = result.portfolio.tail(n)
    else:
        warnings.warn("The portfolio {0} is unknown in the database".format(result.name))
        portfolio = result.portfolio

    return archive.portfolios.update(key=result.name, portfolio=portfolio, group=result.group, comment=result.source)
