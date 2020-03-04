import yaml


def read_config(file):
    with open(file, 'r') as ymlfile:
        configuration = yaml.safe_load(ymlfile)

    return configuration