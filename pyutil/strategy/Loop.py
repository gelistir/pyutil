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

    for module, source in __loop(path=path, prefix=prefix):
        # module
        logger.debug("Module: {0}".format(module))

        config = module.Configuration(archive=archive, t0=t0, logger=logger)
        logger.debug("Name: {0}".format(config.name))
        logger.debug("Group: {0}".format(config.group))

        portfolio = config.portfolio()
        yield Result(name=config.name, portfolio=portfolio, source=source, group=config.group)


def __loop(path, prefix):

    assert isinstance(path, str), "The variable path has to be a str!. It is currently {0}".format(path)
    assert os.path.exists(path=path), "The path {0} does not exist.".format(path)

    for module in pkgutil.iter_modules(path=[path], prefix=prefix):
        name = module[1]

        module = importlib.import_module(name)
        source = inspect.getsource(object=module)
        yield (module, source)

