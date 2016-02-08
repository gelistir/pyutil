from auth import mailgun_api, mailgun_key
from pyutil.log import get_logger, StreamHandler, MailHandler, FileHandler
from pyutil.message import Mail

if __name__ == '__main__':

    m = Mail(mailgunapi=mailgun_api,
             mailgunkey=mailgun_key,
             fromAdr="monitor3@lobnek.com",
             toAdr="thomas.schmelzer@lobnek.com",
             subject="test"
             )


    logger = get_logger("TEST22")
    logger.addHandler(MailHandler(m))
    logger.addHandler(StreamHandler())
    logger.error("WURST2")