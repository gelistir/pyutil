import os
import pkgutil
import importlib
import inspect
import logging


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
        yield {"module": module, "source": source}

