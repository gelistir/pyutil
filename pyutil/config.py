import os
from pyutil.message import Mail


class Configuration(object):
    def __init__(self, file=None):
        file = file or os.path.join(os.path.expanduser("~"), "lobnek.cfg")
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


def mail():
    c = Configuration()
    __api = c["Mailgun"]["mailgunapi"]
    __key = c["Mailgun"]["mailgunkey"]
    return Mail(mailgunapi=__api, mailgunkey=__key)


def mosek():
    c = Configuration()
    __mosek = c["Mosek"]["moseklm_license_file"]
    os.environ.setdefault("MOSEKLM_LICENSE_FILE", __mosek)


