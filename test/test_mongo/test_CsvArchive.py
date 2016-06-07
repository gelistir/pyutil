from pyutil.mongo.reader import CsvArchive
from test.config import read_frame
from unittest import TestCase
import pandas.util.testing as pdt


class TestRunner(TestCase):
    def test_run(self):
        frame = read_frame("price.csv", parse_dates=True)
        archive = CsvArchive(frame)
        f = archive.history(items=archive.keys())
        pdt.assert_frame_equal(f, frame)
