import requests


class Mail(object):
    """
    Class for sending emails with and without attachments via mailgun
    """
    def __init__(self, toAdr, fromAdr, subject, mailgunapi, mailgunkey):
        """
        Create a Mail object
        """

        # make sure that mailgun is of the correct type as specified in the config
        self.__mailgun_api = mailgunapi
        self.__mailgun_key = mailgunkey
        self.__files = list()
        self.__toAdr = toAdr
        self.__fromAdr = fromAdr
        self.__subject = subject

    def attach(self, name, localpath, mode="r+b"):
        """
        attach a file.

        :param name:
        :param localpath:
        :param mode:
        :return: reference to modified object to chain commands
        """
        self.__files.extend([("attachment", (name, open(localpath, mode=mode)))])
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

    def send(self, text):
        """
        send an email

        :param text:
        """
        try:
            return requests.post(self.__mailgun_api, auth=("api", self.__mailgun_key), files=self.__files,
                                 data={"from": self.fromAdr, "to": self.toAdr, "subject": self.subject, "text": text})
        finally:
            for f in self.__files:
                # List of tuples ("attachment or inline", (f[0], f[1]))
                f[1][1].close()

    @property
    def toAdr(self):
        return self.__toAdr

    @property
    def fromAdr(self):
        return self.__fromAdr

    @property
    def subject(self):
        return self.__subject