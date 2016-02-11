import os
from pyutil.message import Mail


class Configuration(object):
    def __init__(self, file=None):
        file = file or os.path.join(os.path.expanduser("~"), "lobnek.cfg")
        assert os.path.exists(file)
        try:
            import configparser
            self.__config = configparser.ConfigParser()
            self.__config.read(file)
            self.__version = 3

        except ImportError:
            import ConfigParser
            self.__config = ConfigParser.ConfigParser()
            self.__config.read(file)
            self.__version = 2

    def __getitem__(self, item):
        if self.__version == 3:
            return {k: x for k, x in self.__config[item].items()}
        else:
            assert self.__config.has_section(item)
            return {key: self.__config.get(item, key) for key in self.__config.options(item)}

    def sections(self):
        return self.__config.sections()


def mail(api=None, key=None, file=None):
    """
    Construct an e-mail

    :param api: The mailgun api string
    :param key: The mailgun api key
    :param file: The config file used to look api and key up if not specified.

    :return: a mail object
    """
    if not api:
        c = Configuration(file=file)
        api = c["Mailgun"]["mailgunapi"]

    if not key:
        c = Configuration(file=file)
        key = c["Mailgun"]["mailgunkey"]

    return Mail(mailgunapi=api, mailgunkey=key)


def mosek(license=None, file=None):
    """
    Set the Mosek environment variable

    :param license: The license location, e.g. location of the file or address of the server
    :param file: The config file used to look the license up if not specified
    """
    if not license:
        c = Configuration(file=file)
        license = c["Mosek"]["moseklm_license_file"]

    os.environ.setdefault("MOSEKLM_LICENSE_FILE", license)
