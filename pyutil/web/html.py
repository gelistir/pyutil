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


def transform(html, base_url):
    from premailer import transform as ttt
    return ttt(html, base_url=base_url)


def compile2html(name, dictionary, folder="./templates", classes="table", base_url="http://quantsrv/"):
    t = getTemplate(name, folder)
    for key, item in dictionary.items():
        dictionary[key] = frame2html(item, classes=classes)

    return transform(t.render(dictionary), base_url=base_url)

#transform(getTemplate(name="fact.html").render({"performance": pp, "monthly": frame, "topflop": topflop, "sector": sector}))