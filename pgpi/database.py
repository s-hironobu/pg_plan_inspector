"""
databse.py


  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 2021-2022, Hironobu Suzuki @ interdb.jp
"""

import sys
import getpass
import configparser
import psycopg2
from .repository import Repository
from .common import Log

"""
This class is a class to connect to databases using the hosts.conf file.

When connecting to a database, there are two ways to set a user's password:

(1) Set 'input_password' to 'true' in the hosts.conf file.
   In this case, prompt to enter the password whenever connecting to the database.

(2) Write passwords to the 'password' key in the hosts.conf file.

"""


class Database(Repository):
    def __init__(self, base_dir=".", log_level=Log.info):
        self.set_base_dir(base_dir)
        self.LogLevel = log_level

    """
    Public methods
    """

    def get_connection_param(self, serverId, database="postgres"):
        """Return a connection param using the values in the hosts.conf."""

        if self.check_serverId(serverId) == False:
            if Log.error <= self.LogLevel:
                print("Error: serverId '{}' is not registered.".format(serverId))
            sys.exit(1)

        _conn = ""
        _config = configparser.ConfigParser()
        _config.read(self.get_conf_file_path())
        for _section in _config.sections():
            if _section == serverId:
                _conn = (
                    "host="
                    + str(_config[_section]["host"])
                    + " port="
                    + str(_config[_section]["port"])
                )
                _conn += (
                    " dbname="
                    + database
                    + " user="
                    + str(_config[_section]["username"])
                )

                """
                If 'input_password' is True, prompt to enter the password.
                """
                if "input_password" in _config[_section]:
                    if str.lower(_config[_section]["input_password"]) == "true":
                        _password = getpass.getpass()
                        _conn += " password=" + _password
                        return _conn

                """
                If there is a password value, append the password to
                the connection param.
                """
                if "password" in _config[_section]:
                    if _config[_section]["password"]:
                        _conn += " password=" + str(_config[_section]["password"])

        return _conn

    def connect(self, serverId, database="postgres"):
        """Connect to the database server specified by serverId."""

        _conn = self.get_connection_param(serverId, database)
        try:
            _connection = psycopg2.connect(_conn)
        except psycopg2.OperationalError as e:
            print("Error: Could not connect to '{}'".format(serverId))
            sys.exit(1)
        _connection.autocommit = True
        return _connection
