import logging
import os

__format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
__level = logging.DEBUG


def StreamHandler(stream=None, level=None, format=None):
    """
    Streamhandler, provides handler.stream.getvalue()
    """
    handler = logging.StreamHandler(stream)
    handler.level = level or __level
    handler.formatter = logging.Formatter(format or __format)
    return handler


def FileHandler(file, level=logging.INFO, format=None, mode="a"):
    """
    Filehandler, logs to a file

    :param level:
    :param format:
    :param file:
    :param mode:
    """

    handler = logging.FileHandler(filename=file, mode=mode)
    handler.level = level or __level
    handler.formatter = logging.Formatter(format or __format)
    return handler


def SentryHandler(dns=None, level=logging.WARNING):
    from raven.handlers.logging import SentryHandler
    dns = dns or os.environ["SENTRY"]
    sentry_handler = SentryHandler(dns)
    sentry_handler.level = level
    return sentry_handler


def get_logger(name="LWM", level=None, format=None):
    """
    Provides a basic logger, append handlers to this logger

    :param format:
    :param level:
    :param name:
    """
    logging.basicConfig(level=level or __level, format=format or __format)
    return logging.getLogger(name)
