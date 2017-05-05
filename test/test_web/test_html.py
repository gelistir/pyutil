from unittest import TestCase

import pandas as pd

from pyutil.web.html import compile2html

transformed="""<!DOCTYPE html>
<html lang="en">

<head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <meta name="viewport" content="width=device-width">
    <style type="text/css">tbody tr:nth-child(even) {background-color:#f2f2f2}</style>
</head>

<body>
     <table border="0" class="dataframe table" style="border-collapse:collapse; width:100%" width="100%">
  <thead>
    <tr style="text-align: right;">
      <th style="border:1px solid #ddd; padding:8px; text-align:left; background-color:#4CAF50; color:white" align="left" bgcolor="#4CAF50"></th>
      <th style="border:1px solid #ddd; padding:8px; text-align:left; background-color:#4CAF50; color:white" align="left" bgcolor="#4CAF50">0</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th style="border:1px solid #ddd; padding:8px; text-align:left; background-color:#4CAF50; color:white" align="left" bgcolor="#4CAF50">0</th>
      <td style="border:1px solid #ddd; padding:8px; text-align:left" align="left">2.00</td>
    </tr>
  </tbody>
</table>
</body>
</html>
"""

class TestHtml(TestCase):
    def test_compile2html(self):
        d = dict()
        d["latest"] = pd.DataFrame(data=[[2.0]])
        f = compile2html(file="/pyutil/test/resources/templates/latest.html", render_dict=d)
        self.assertEqual(f, transformed)





