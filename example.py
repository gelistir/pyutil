from os.path import expanduser
from os.path import join

from pyutil.config import Configuration, mail
from pyutil.log import get_logger, StreamHandler, MailHandler


if __name__ == '__main__':
    # this is one way to get the data needed to hookup with the restful API of Mailgun.
    # Obviously we have to protect our configuration file. So this code will fail on your local machine
    # unless you have the file lobnek.cfg in your homepath
    config = Configuration(file=join(expanduser("~"), "lobnek.cfg"))
    __api = config["Mailgun"]["mailgunapi"]
    __key = config["Mailgun"]["mailgunkey"]

    # the cfg file may look like
    # [Mailgun]
    # MAILGUNAPI = https://api.mailgun.net/v2/...
    # MAILGUNKEY = key-2....

    # you could do
    m = mail(api=__api, key=__key)

    # or
    m = mail(file=join(expanduser("~"), "lobnek.cfg"))

    # or
    m = mail()
    # in this case the program looks up the key + api in the file "~/lobnek.cfg", which you may not have...

    m.fromAdr = "t@mailinator.com"
    m.toAdr = "t@mailinator.com"
    m.subject = "test"

    # The library also provides tools for logging...
    logger = get_logger("TEST")
    logger.addHandler(MailHandler(m))
    logger.addHandler(StreamHandler())
    logger.error("I shot the sheriff.")



