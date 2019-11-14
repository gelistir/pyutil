import configparser
import os


def config(file=os.environ["APPLICATION_SETTINGS"]):
    with open(file=file) as f:
        __file_content = '[SETTINGS]\n' + f.read()
        __config_parser = configparser.RawConfigParser()
        __config_parser.read_string(__file_content)

        return dict(__config_parser["SETTINGS"])