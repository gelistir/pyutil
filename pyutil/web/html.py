import pandas as pd


def frame2html(frame, classes="table"):
    return frame.to_html(classes=classes, float_format=lambda x: '{0:.2f}'.format(x) if pd.notnull(x) else '-',
                         escape=False).replace('border="1"', 'border="0"')


def link(name):
    x = name.lstrip().split(" ")
    assert len(x) >= 1, "Problem with {0}".format(name)
    return "<a href=http://www.bloomberg.com/quote/{0}:{1}>{2}</a>".format(x[0], x[1], name.lstrip())


def getTemplate(name, folder="./templates/"):
    from jinja2 import Environment, FileSystemLoader
    env = Environment(loader=FileSystemLoader(folder))
    return env.get_template(name)


def transform(html):
    from premailer import transform as ttt
    return ttt(html)
