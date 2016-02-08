from unittest import TestCase
from pyutil.config import Configuration

config = Configuration()

class TestConfig(TestCase):
    def test_sections(self):

        assert "Mailgun" in config.sections()
        assert "Database" in config.sections()
        assert "Mosek" in config.sections()
        assert "SQL-Read" in config.sections()
        assert "SQL-Write" in config.sections()

    def test_mailgun(self):
        assert config["Mailgun"]["mailgun"]
