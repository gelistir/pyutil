import inspect


def function_dict(module):
    """
    Dictionary of functions defined in a module. Neat way to iterate over the functions
    in a file from __main__

    :param module: the module..., e.g. sys.modules[__name__]

    :return: dict {name: function}
    """
    name_func_tuples = inspect.getmembers(module, inspect.isfunction)
    name_func_tuples = [t for t in name_func_tuples if inspect.getmodule(t[1]) == module]
    return dict(name_func_tuples)