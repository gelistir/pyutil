import logging

# let's soon ditch support for Python 2.
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


from pyutil.message import Mail

__format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
__level = logging.DEBUG


def MailHandler(mail, level=None, format=None):
    """
    MailHandler, sending log messages directly via email

    :param mail: That's the mail object as defined in
    :param level:
    :param format:

    :return: the handler
    """
    class mailhandler(logging.Handler):
        def emit(self, record):
            self.__mail.send(text=self.format(record))

        def __init__(self, mail, format, level):
            try:
                super().__init__()
            except:
                super(mailhandler, self).__init__()

            assert isinstance(mail, Mail)
            self.__mail = mail
            self.level = level
            self.formatter = logging.Formatter(format)

    return mailhandler(mail, level=level or __level, format=format or __format)


def StreamHandler(level=None, format=None):
    """
    Streamhandler, provides handler.stream.getvalue()
    """
    handler = logging.StreamHandler(StringIO())
    handler.level = level or __level
    handler.formatter = logging.Formatter(format or __format)
    return handler


def FileHandler(file, level=None, format=None, mode="w+"):
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


def get_logger(name, level=None, format=None):
    """
    Provides a basic logger, append handlers to this logger

    :param format:
    :param level:
    :param name:
    """
    logging.basicConfig(level=level or __level, format=format or __format)
    return logging.getLogger(name)
