import yaml


def read_config(file):
    # parse a yaml file
    with open(file, 'r') as yml_file:
        configuration = yaml.safe_load(yml_file)

    return configuration
