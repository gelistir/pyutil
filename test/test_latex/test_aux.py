import pandas as pd
import tempfile
from pyutil.latex.aux import formatter, to_latex

from unittest import TestCase


class TestLatex(TestCase):
    def testFormatter(self):
        f = formatter("{:0.3f}")
        self.assertEqual(f(0.12345), "0.123")
        self.assertEqual(f(0.1), "0.100")
        self.assertEqual(f(0.12390), "0.124")

    def testToLatex(self):
        file = tempfile.NamedTemporaryFile()
        frame = pd.DataFrame(data=[[2.0, 3.0], [1.0, 4.0]])
        to_latex(frame=frame, filename=file.name)

        with open(file.name, "r") as file:
            lines = [line for line in file.readlines()]

        self.assertListEqual(lines, ['\\begin{tabular}{lrr}\n', '\\toprule\n', '{} &    0 &    1 \\\\\n', '\\midrule\n', '0 & 2.00 & 3.00 \\\\\n', '1 & 1.00 & 4.00 \\\\\n', '\\bottomrule\n', '\\end{tabular}\n'])

