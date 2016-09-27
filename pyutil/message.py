import logging

import requests
import os


def mail(mailgunapi=None, mailgunkey=None):
    mailgunapi = mailgunapi or os.environ["MAILGUNAPI"]
    mailgunkey = mailgunkey or os.environ["MAILGUNKEY"]
    return _Mail(mailgunapi=mailgunapi, mailgunkey=mailgunkey)


class _Mail(object):
    """
    Class for sending emails with and without attachments via mailgun
    """

    def __init__(self, mailgunapi, mailgunkey, toAdr=None, fromAdr=None, subject=None):
        """
        Create a Mail object
        """
        # make sure that mailgun is of the correct type as specified in the config
        self.__mailgun_api = mailgunapi
        self.__mailgun_key = mailgunkey
        self.__files = list()
        self.__toAdr = toAdr or "lwm@lobnek.com"
        self.__fromAdr = fromAdr or "monitor@lobnek.com"
        self.__subject = subject or ""

        #self.__logger = logger or logging.getLogger("LWM")

    def clear(self):
        # remove all attachments
        self.__files = list()
        self.__subject = ""

    def attach_file(self, name, localpath, mode="r+b"):
        """
        attach a file.

        :param name:
        :param localpath:
        :param mode:
        :return: reference to modified object to chain commands
        """
        self.__files.extend([("attachment", (name, open(localpath, mode=mode)))])
        return self

    def attach_stream(self, name, stream):
        """
        attach a stream (or string).

        :param name:
        :param frame:
        :return: reference to modified object to chain commands
        """
        self.__files.extend([("attachment", (name, stream))])
        return self

    def inline(self, name, localpath, mode="r+b"):
        """
        inline a file.

        :param name:
        :param localpath:
        :param mode:
        :return: reference to modified object to chain commands
        """
        self.__files.extend([("inline", (name, open(localpath, mode=mode)))])
        return self

    def inline_stream(self, name, stream):
        """
        inline a file.

        :param name:
        :param stream:

        :return: reference to modified object to chain commands
        """
        self.__files.extend([("inline", (name, stream))])
        return self

    def send(self, text="", html="", logger=None):
        """
        send an email

        :param text:
        """
        logger = logger or logging.getLogger(__name__)
        try:
            data = {"from": self.fromAdr, "to": self.toAdr, "subject": self.subject, "text": text, "html": '<font face="Courier New, Courier, monospace">' + html + '</font>'}
            logger.info("Mail: {0}".format(data))
            for file in self.__files:
                logger.info("type: {0}, name: {1}".format(file[0], file[1][0]))

            return requests.post(self.__mailgun_api, auth=("api", self.__mailgun_key), files=self.__files, data=data)

        finally:
            for f in self.__files:
                # List of tuples ("attachment or inline", (f[0], f[1]))
                try:
                    f[1][1].close()
                except:
                    pass

    @property
    def toAdr(self):
        return self.__toAdr

    @property
    def fromAdr(self):
        return self.__fromAdr

    @property
    def subject(self):
        return self.__subject

    @subject.setter
    def subject(self, value):
        self.__subject = value

    @fromAdr.setter
    def fromAdr(self, value):
        self.__fromAdr = value

    @toAdr.setter
    def toAdr(self, value):
        self.__toAdr = value
