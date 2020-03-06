import logging
import logging.config

from .yaml import read_config


def log_config(file):
    config = read_config(file)

    assert len(config["loggers"].keys()) == 1
    names = list(config["loggers"].keys())
    logging.config.dictConfig(config)

    # return the logger just defined
    return logging.getLogger(name=names[0])