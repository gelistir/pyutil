import logging

from pyutil.decorators import attempt
from pyutil.mongo.archive import reader

# I use the attempt decorator here. The attempt decorator embeds the function into try/except and reports any exception
# to the logger...:

@attempt
def f(logger, archive):
    for key in archive.portfolios.keys():
        logger.debug(key)
        portfolio = archive.portfolios[key]
        print(portfolio.nav.monthlytable)


if __name__ == '__main__':
    f(logger=logging.getLogger(__name__), archive=reader(name="production", host="quantsrv"))
