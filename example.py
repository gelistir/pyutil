from os.path import expanduser
from os.path import join

from pyutil.config import Configuration
from pyutil.log import get_logger, StreamHandler, MailHandler
from pyutil.message import Mail

if __name__ == '__main__':
    # this is one way to get the data needed to hookup with the restful API of Mailgun.
    # Obviously we have to protect our configuration file. So this code will fail on your local machine
    # unless you have the file lobnek.cfg in your homepath
    config = Configuration(file=join(expanduser("~"), "lobnek.cfg"))
    __api = config["Mailgun"]["mailgunapi"]
    __key = config["Mailgun"]["mailgunkey"]

    m = Mail(mailgunapi=__api, mailgunkey=__key, fromAdr="t@mailinator.com", toAdr="t@mailinator.com", subject="test")

    logger = get_logger("TEST")
    logger.addHandler(MailHandler(m))
    logger.addHandler(StreamHandler())
    logger.error("I shot the sheriff.")
