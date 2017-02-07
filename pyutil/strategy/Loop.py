import os
import pkgutil
import importlib
import inspect
import logging

from collections import namedtuple

import pandas as pd

Result = namedtuple('Result', ['name', 'portfolio'])


def loop_configurations(reader, path, prefix, logger=None):
    logger = logger or logging.getLogger(__name__)
    logger.info("Search Path: {0}".format(path))
    logger.info("Prefix: {0}".format(prefix))

    for module, source in __loop(path=path, prefix=prefix):
        # module
        logger.debug("Module: {0}".format(module))

        config = module.Configuration(reader=reader, logger=logger)
        logger.debug("Name: {0}".format(config.name))
        logger.debug("Group: {0}".format(config.group))

        portfolio = config.portfolio()
        portfolio.meta["group"] = config.group
        portfolio.meta["comment"] = source
        portfolio.meta["time"] = pd.Timestamp("now")

        yield Result(name=config.name, portfolio=portfolio)


def __loop(path, prefix):

    assert isinstance(path, str), "The variable path has to be a str!. It is currently {0}".format(path)
    assert os.path.exists(path=path), "The path {0} does not exist.".format(path)

    for module in pkgutil.iter_modules(path=[path], prefix=prefix):
        name = module[1]

        module = importlib.import_module(name)
        source = inspect.getsource(object=module)
        yield (module, source)

