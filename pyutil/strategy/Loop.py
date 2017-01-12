import os
import pkgutil
import importlib
import inspect
import logging

from collections import namedtuple
Result = namedtuple('Result', ['name', 'portfolio', 'source', 'group'])


def loop_configurations(archive, t0, path, prefix, logger=None):
    logger = logger or logging.getLogger(__name__)
    logger.info("Search Path: {0}".format(path))
    logger.info("Prefix: {0}".format(prefix))

    for module, source in loop(path=path, prefix=prefix, logger=logger):
        #module = x["module"]
        #source = x["source"]

        config = module.Configuration(archive=archive, t0=t0, logger=logger)
        portfolio = config.portfolio()

        yield Result(name=config.name, portfolio=portfolio, source=source, group=config.group)


def loop(path, prefix, logger=None):
    logger = logger or logging.getLogger(__name__)
    logger.info("Search Path: {0}".format(path))
    logger.info("Prefix: {0}".format(prefix))

    assert isinstance(path, str), "The variable path has to be a str!. It is currently {0}".format(path)
    assert os.path.exists(path=path), "The path {0} does not exist.".format(path)

    for module in pkgutil.iter_modules(path=[path], prefix=prefix):
        name = module[1]
        logger.info("Module : {0}".format(name))

        module = importlib.import_module(name)
        source = inspect.getsource(object=module)
        yield (module, source)

