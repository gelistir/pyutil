import logging
from pyutil.config.logging import log_config
from test.config import resource


def test_logging():
    file = resource(name="logging.yaml")
    x = log_config(file=file)
    assert isinstance(x, logging.Logger)
