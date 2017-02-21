import contextlib
import logging

@contextlib.contextmanager
def parent(log=None):
    log = log or logging.getLogger(__name__)
    try:
        yield {}
    except Exception as e:
        log.exception(e)
        raise
    finally:
        log.debug("Bye")