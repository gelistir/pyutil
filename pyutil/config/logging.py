import logging
import logging.config
import ruamel_yaml


def log_config(file):
    with open(file, 'rt') as f:
        config = ruamel_yaml.safe_load(f.read())

        assert len(config["loggers"].keys()) == 1
        names = list(config["loggers"].keys())
        logging.config.dictConfig(config)

        # return the logger just defined
        return logging.getLogger(name=names[0])