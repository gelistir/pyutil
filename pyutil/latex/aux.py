def formatter(str="{:0.2f}"):
    return lambda x: str.format(x)


def to_latex(frame, filename, na_rep="", float_format=lambda number: "{:0.2f}".format(number)):
    with open(filename, "w") as file:
        frame.to_latex(buf=file, na_rep=na_rep, float_format=float_format, escape=False)