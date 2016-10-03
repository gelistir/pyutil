import logging


def attempt(function):
    # the wrapper provides an "environment" to execute the function "function"
    # The function shall offer a logger as it's first argument
    def wrapper(logger=None, **kwargs):
        # create the logger if it wasn't used in the call of the function
        logger = logger or logging.getLogger(__name__)
        try:
            return function(logger, **kwargs)
        except Exception as e:
            logger.exception(e)
            raise

    # return a "function pointer"
    return wrapper