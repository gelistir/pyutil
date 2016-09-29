import logging


def attempt(function):
    def wrapper(logger=None, *args, **kwargs):
        try:
            logger = logger or logging.getLogger(__name__)
            return function(logger, *args, **kwargs)
        except Exception as e:
            logger.exception(e)
            raise

    return wrapper