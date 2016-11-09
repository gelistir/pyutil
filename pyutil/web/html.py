import os
import pandas as pd
import jinja2
from premailer import transform


def __frame2html(frame, classes="table"):
    return frame.to_html(classes=classes, float_format=lambda x: '{0:.2f}'.format(x) if pd.notnull(x) else '-',
                         escape=False).replace('border="1"', 'border="0"')


def link(name: str) -> str:
    x = name.lstrip().split(" ")
    assert len(x) >= 1, "Problem with {0}".format(name)
    return "<a href=http://www.bloomberg.com/quote/{0}:{1}>{2}</a>".format(x[0], x[1], name.lstrip())


def __getTemplate(tpl_path):
    path, filename = os.path.split(tpl_path)
    return jinja2.Environment(loader=jinja2.FileSystemLoader(path or './')).get_template(filename)


def compile2html(file, render_dict=None, classes="table", base_url="http://quantsrv/"):
    assert os.path.exists(file), "File {0} does not exist.".format(file)
    t = __getTemplate(tpl_path=file)
    if render_dict:
        for key, item in render_dict.items():
            if isinstance(item, pd.DataFrame):
                render_dict[key] = __frame2html(item, classes=classes)

        return transform(t.render(render_dict), base_url=base_url)
    else:
        return transform(t, base_url=base_url)