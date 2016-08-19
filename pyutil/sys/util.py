import inspect


def inspect(module):
    name_func_tuples = inspect.getmembers(module, inspect.isfunction)
    name_func_tuples = [t for t in name_func_tuples if inspect.getmodule(t[1]) == module]
    return dict(name_func_tuples)