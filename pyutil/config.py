from os.path import join, expanduser


class Configuration(object):

    def __init__(self, file=None):
        file = file or join(expanduser("~"), "lobnek.cfg")
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


if __name__ == '__main__':
    c = Configuration()
    print(c["Mailgun"]["mailgunkey"])
    print(c.sections())

