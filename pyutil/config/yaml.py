import yaml


def read_config(file):
    with open(file, 'r') as yml_file:
        configuration = yaml.safe_load(yml_file)

    return configuration
