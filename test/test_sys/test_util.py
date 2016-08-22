from unittest import TestCase

from pyutil.sys.util import function_dict
import test.config


class TestUtil(TestCase):
    def test_function_dict(self):
        assert "read_series" in function_dict(module=test.config)


