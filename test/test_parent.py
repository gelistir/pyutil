from unittest import TestCase

import io

from pyutil.parent import Production, get_stream_handler


class TestDecorator(TestCase):

    def test_production(self):
        f = io.StringIO()
        with Production() as p:
            self.assertIsNotNone(p.logger)
            p.logger.addHandler(get_stream_handler(stream=f, format="%(name)s - %(levelname)s - %(message)s"))
            p.logger.warning("Hello Peter Maffay")
            f.flush()
            self.assertEqual(f.getvalue(), "LWM - WARNING - Hello Peter Maffay\n")

