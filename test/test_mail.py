from pyutil.config import mail
from unittest import TestCase


class TestMail(TestCase):
    def test_mail(self):
        m = mail()
        m.toAdr = "Peter.Maffay@mailinator.com"
        m.fromAdr = "Peter.Maffay@mailinator.com"
        m.subject = "Ich wollte nie erwachsen sein"
        m.attach_stream("test.csv", "A,B,C")
        data = m.send(text="Du")
        self.assertEqual(data.status_code, 200)

    def test_mail_inline(self):
        m = mail()
        m.toAdr = "Peter.Maffay@mailinator.com"
        m.fromAdr = "Peter.Maffay@mailinator.com"
        m.subject = "Ich wollte nie erwachsen sein"
        m.inline_stream("test.csv", "A,B,C")
        data = m.send(text="Du")
        self.assertEqual(data.status_code, 200)