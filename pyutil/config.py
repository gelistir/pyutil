import os
from pyutil.message import Mail


def configuration(file=None):
    file = file or os.path.join(os.path.expanduser("~"), "lobnek.cfg")
    assert os.path.exists(file)
    try:
        import configparser
        config = configparser.ConfigParser()
        config.read(file)
        return {item: {k: x for k,x in config[item].items()} for item in config.sections()}

    except ImportError:
        import ConfigParser
        config = ConfigParser.ConfigParser()
        config.read(file)
        return {item: {key: config(item, key) for key in config.options(item)} for item in config.sections()}


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


def session(write=False, connect=None, file=None):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    if write:
        if not connect:
            c = configuration(file=file)
            connect = c["SQL-Write"]["connect"]
    else:
         if not connect:
            c = configuration(file=file)
            connect = c["SQL-Read"]["connect"]

    __ENGINE = create_engine(connect, encoding="utf8", echo=False)
    return Session(__ENGINE)

if __name__ == '__main__':
    print(configuration())