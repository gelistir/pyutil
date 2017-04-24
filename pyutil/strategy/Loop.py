import os
import pkgutil
import importlib
import inspect
import logging

import pandas as pd


def loop_configurations(reader, path, prefix, logger=None):
    """
    This function is right at the core of all our trading systems.
    
    :param reader: Is a function that takes a string argument and returns an Asset (object)
    :param path: The path where the scripts live... Use an absolute path to be safe
    :param prefix: The prefix used when loading those scripts
    :param logger: A logger (object)
    
    :return: Yields(!) configuration name and the computed portfolio 
    """
    logger = logger or logging.getLogger(__name__)
    logger.info("Search Path: {0}".format(path))
    logger.info("Prefix: {0}".format(prefix))

    assert isinstance(path, str), "The variable path has to be a str!. It is currently {0}".format(path)
    assert os.path.exists(path=path), "The path {0} does not exist.".format(path)

    for m in pkgutil.iter_modules(path=[path], prefix=prefix):
        # module
        module = importlib.import_module(m.name)
        logger.debug("Module: {0}".format(module))

        config = module.Configuration(reader=reader, logger=logger)
        logger.debug("Name: {0}".format(config.name))
        logger.debug("Group: {0}".format(config.group))

        portfolio = config.portfolio()
        portfolio.meta["group"] = config.group
        portfolio.meta["comment"] = inspect.getsource(object=module)
        portfolio.meta["time"] = pd.Timestamp("now")

        yield config.name, portfolio
