import configparser
import pathlib
import os

file_parent_directory = pathlib.Path(__file__).parent.resolve()

config = configparser.ConfigParser()
config.read(os.path.join(file_parent_directory, 'config.ini'))
HOST=config["elasticsearch"]["HOST"]
USER=config["elasticsearch"]["USER"]
PASSWORD=config["elasticsearch"]["PASSWORD"]
SCHEME=config["elasticsearch"]["SCHEME"]
PORT=config["elasticsearch"]["PORT"]