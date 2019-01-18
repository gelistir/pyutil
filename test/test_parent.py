import pytest

from pyutil.parent import Production


class TestDecorator(object):
    def test_production(self):
        with Production() as p:
            assert p.logger
            p.logger.warning("Hello Peter Maffay")

    def test_production_error(self):
        with pytest.raises(AssertionError):
            with Production() as p:
                raise AssertionError
