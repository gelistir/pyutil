from unittest import TestCase

import pandas as pd

from pyutil.web.html import transform, link, getTemplate, compile2html


parse="""<!DOCTYPE html>
<html lang="en">

<head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
    <meta name="viewport" content="width=device-width" />
    <link rel="stylesheet" href="/pyutil/test/resources/templates/style.css">
</head>

<body>
     2.0
</body>
</html>"""

transformed="""<!DOCTYPE html>
<html lang="en">

<head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <meta name="viewport" content="width=device-width">
    <style type="text/css">tbody tr:nth-child(even) {background-color:#f2f2f2}</style>
</head>

<body>
     2.0
</body>
</html>
"""

class TestHtml(TestCase):
    def test_link(self):
        self.assertEqual(link("CARMPAT FP Equity"), "<a href=http://www.bloomberg.com/quote/CARMPAT:FP>CARMPAT FP Equity</a>")

    def test_getTemplate(self):
        x = getTemplate("latest.html", folder="/pyutil/test/resources/templates")
        print(x.render({"latest": 2.0}))
        self.assertEqual(x.render({"latest": 2.0}), parse)

    def test_transform(self):
        x = getTemplate("latest.html", folder="/pyutil/test/resources/templates")
        self.assertEqual(transform(x.render({"latest": 2.0})), transformed)

    def test_compile2html(self):
        d = dict()
        d["latest"] = pd.DataFrame(data=[[2.0]])
        f = compile2html(name="latest.html", dictionary=d, folder="/pyutil/test/resources/templates")





