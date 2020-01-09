import pytest

from pyutil.parent import Production


def test_production():
    with Production() as p:
        assert p.logger
        p.logger.warning("Hello Peter Maffay")


def test_production_error():
    with pytest.raises(AssertionError):
        with Production() as p:
            raise AssertionError
