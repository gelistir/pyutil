import os
from pyutil.message import Mail


def configuration(file=None):
    """
    Construct a dictionary of dictionaries from a config file. One dictionary per section

    :param file:
    :return:
    """
    file = file or os.path.join(os.path.expanduser("~"), "lobnek.cfg")
    assert os.path.exists(file)
    try:
        # Python 3
        import configparser
        config = configparser.ConfigParser()
        config.read(file)
        return {section: {k: x for k,x in config[section].items()} for section in config.sections()}

    except ImportError:
        # Python 2
        import ConfigParser
        config = ConfigParser.ConfigParser()
        config.read(file)
        return {section: {k: config.get(section, k) for k in config.options(section)} for section in config.sections()}


def mail(api=None, key=None, file=None):
    """
    Construct an e-mail

    :param api: The mailgun api string
    :param key: The mailgun api key
    :param file: The config file used to look api and key up if not specified.

    :return: a mail object
    """
    if not api:
        c = configuration(file=file)
        api = c["Mailgun"]["mailgunapi"]

    if not key:
        c = configuration(file=file)
        key = c["Mailgun"]["mailgunkey"]

    return Mail(mailgunapi=api, mailgunkey=key)


def mosek(license=None, file=None):
    """
    Set the Mosek environment variable

    :param license: The license location, e.g. location of the file or address of the server
    :param file: The config file used to look the license up if not specified
    """
    if not license:
        c = configuration(file=file)
        license = c["Mosek"]["moseklm_license_file"]

    os.environ.setdefault("MOSEKLM_LICENSE_FILE", license)

