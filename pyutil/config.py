import os
from pyutil.message import Mail


# def configuration(file=None):
#     """
#     Construct a dictionary of dictionaries from a config file. One dictionary per section
#
#     :param file:
#     :return:
#     """
#     file = file or os.path.join(os.path.expanduser("~"), "lobnek.cfg")
#     assert os.path.exists(file), "The file {0} does not exist".format(file)
#     try:
#         # Python 3
#         import configparser
#         config = configparser.ConfigParser()
#         config.read(file)
#         return {section: {k: x for k,x in config[section].items()} for section in config.sections()}
#
#     except ImportError:
#         # Python 2
#         import ConfigParser
#         config = ConfigParser.ConfigParser()
#         config.read(file)
#         return {section: {k: config.get(section, k) for k in config.options(section)} for section in config.sections()}


# def mail(api=None, key=None):
#     """
#     Construct an e-mail
#
#     :param api: The mailgun api string
#     :param key: The mailgun api key
#
#     :return: a mail object
#     """
#     if not api:
#         api = os.environ["Mailgunapi"]
#
#     if not key:
#         key = os.environ["Mailgunkey"]
#
#     return Mail(mailgunapi=api, mailgunkey=key)


# def postgresql(server=None, database=None, password=None, port=None):
#
#     if not server:
#         c = configuration(file=file)
#         server = c["Postgresql"]["server"]
#
#     if not database:
#         c = configuration(file=file)
#         database = c["Postgresql"]["database"]
#
#     if not password:
#         c = configuration(file=file)
#         password = c["Postgresql"]["password"]
#
#     if not port:
#         c = configuration(file=file)
#         port = c["Postgresql"]["port"]
#
#     return 'postgresql://postgres:{2}@{0}:{3}/{1}'.format(server, database, password, port)
