import logging
import logging.config

from .yaml import read_config


def log_config(file):
    config = read_config(file)

    # we allow only one logger per file
    assert len(config["loggers"].keys()) == 1
    # and he shall have the name...
    name = list(config["loggers"].keys())[0]
    # configure all the handlers and formatters
    logging.config.dictConfig(config)

    # return the logger just defined
    return logging.getLogger(name=name)

# some example:

# version: 1
# disable_existing_loggers: true
#
# formatters:
#   simple:
#     format: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# handlers:
#   console:
#     class: logging.StreamHandler
#     level: DEBUG
#     formatter: simple
#     stream: ext://sys.stdout
# loggers:
#   lobnek:
#     level: DEBUG
#     handlers: [console]
#     propagate: no
