from pyutil.config import configuration, mail, postgresql, mosek
from unittest import TestCase
from test.config import BASE_DIR
from pyutil.message import Mail

import os
file = os.path.join(BASE_DIR, "resources", "test.cfg")


class TestConfig(TestCase):
    def test_configuration(self):
        x = configuration(file=file)
        self.assertTrue("Mailgun" in x)

    def test_mail(self):
        m = mail(file=file)
        self.assertTrue(isinstance(m, Mail))

    def test_postgresql(self):
        m = postgresql(file=file)
        self.assertEqual(m, "postgresql://postgres:a@server:111/x")

    def test_mosek(self):
        m = mosek(file=file)
        self.assertEqual(m, "a")

