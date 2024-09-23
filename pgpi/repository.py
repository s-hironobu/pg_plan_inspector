"""
repository.py


  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 2021-2024, Hironobu Suzuki @ interdb.jp
"""

import configparser
import glob
import shutil
import sys
import csv
import os
import re

from .common import Common, Log


class Repository(Common):
    def __init__(self, base_dir=".", log_level=Log.error):
        self.set_base_dir(base_dir)
        self.LogLevel = log_level

    DEFAULT_DIR_MODE = 0o770
    DEFAULT_HOSTS_CONF_MODE = 0o640

    def secure_check(self, path, ref_mode):
        if os.path.exists(path) == False:
            if Log.notice <= self.LogLevel:
                print("Notice: '{}' is not found.".format(path))
            return True
        MASK = int(0o777)
        _mode = int(os.stat(path).st_mode & MASK)
        _mode |= ref_mode
        _mode ^= ref_mode
        return True if int(_mode) == 0 else False

    """
    stat file related functions.
    """

    def __create_stat_file(self, serverId, path):
        stat = configparser.ConfigParser()
        stat[serverId] = {"seqid": "0"}
        with open(path, "w") as configfile:
            stat.write(configfile)

    def __update_stat_file(self, serverId, max_seqid, _dir):
        _dirpath = self.dirpath([serverId, _dir])
        _path = self.path(_dirpath, self.STAT_FILE)

        stat = configparser.ConfigParser()
        stat[serverId] = {"seqid": max_seqid}
        with open(_path, "w") as configfile:
            stat.write(configfile)

    def __get_seqid_from_stat_file(self, serverId, _dir):
        _dirpath = self.dirpath([serverId, _dir])
        _path = self.path(_dirpath, self.STAT_FILE)

        if os.path.exists(_dirpath):
            stat = configparser.ConfigParser()
            stat.read(_path)
            if stat[serverId]:
                return int(stat[serverId]["seqid"])
        else:
            return int(0)

    """
    Check and create directory if not found.
    """

    def __check_dir(self, serverId, _dir, additional_dir_list):
        _dirpath = self.dirpath([serverId])
        if os.path.exists(_dirpath) == False:
            os.mkdir(_dirpath, self.DEFAULT_DIR_MODE)
        _dirpath = self.dirpath([serverId, _dir])
        _path = self.path(_dirpath, self.STAT_FILE)
        if os.path.exists(_dirpath) == False:
            os.mkdir(_dirpath, self.DEFAULT_DIR_MODE)
            for d in additional_dir_list:
                os.mkdir(_dirpath + d, self.DEFAULT_DIR_MODE)
            self.__create_stat_file(serverId, _path)

    def __reset_dir(self, serverId, _dir, update_stat_file):
        if self.check_serverId(serverId) == False:
            if Log.error <= self.LogLevel:
                print("Error: serverId '{}' is not registered.".format(serverId))
            sys.exit(1)

        _rsdirpath = self.dirpath([serverId, _dir])
        if os.path.exists(_rsdirpath):
            if _dir == self.TABLES_DIR:
                if Log.debug2 <= self.LogLevel:
                    print("Debug2: rm dir '{}'".format(_rsdirpath))
                shutil.rmtree(_rsdirpath)
                # update_stat_file(serverId, 0)
            else:
                _d = str(_rsdirpath) + "/" + "[0-9][0-9][0-9]"
                _dirs = glob.glob(_d, recursive=True)
                for _dir in _dirs:
                    if Log.debug2 <= self.LogLevel:
                        print("Debug2: rm '{}'".format(_dir))
                    shutil.rmtree(_dir)
                update_stat_file(serverId, 0)

    """
    Public methods
    """

    def set_base_dir(self, base_dir="."):
        self.base_dir = base_dir + "/"

    def get_conf_file_path(self):
        _path = self.base_dir + self.REPOSITORY_DIR + "/" + self.CONF_FILE
        if os.path.exists(_path):
            if self.secure_check(_path, self.DEFAULT_HOSTS_CONF_MODE) == False:
                print(
                    "Error: {}'s mode should be set to {} or more secure.".format(
                        self.CONF_FILE, oct(self.DEFAULT_HOSTS_CONF_MODE)
                    )
                )
                sys.exit(1)
        return _path

    def check_serverId(self, serverId):
        if self.is_serverId_valid(serverId) == False:
            if Log.error <= self.LogLevel:
                print("Error: serverId='{}' is invalid.".format(serverId))
                print("\tserverId must be the following regular expression:[A-z0-9_]+")
            sys.exit(1)
        _path = self.get_conf_file_path()
        config = configparser.ConfigParser()
        config.read(_path)
        return config.has_section(serverId)

    def get_serverId(self, host, port):
        _path = self.get_conf_file_path()
        _config = configparser.ConfigParser()
        _config.read(_path)
        _ret = None
        for section in _config.sections():
            if "host" in _config[section] and "port" in _config[section]:
                if (
                    _config[section]["host"] == host
                    and _config[section]["port"] == port
                ):
                    _ret = section
                    break
        return _ret

    def dirpath(self, dirlist):
        _dir = self.base_dir + self.REPOSITORY_DIR + "/"
        if isinstance(dirlist, list):
            for d in dirlist:
                _dir += d + "/"
        else:
            _dir += dirlist + "/"
        return _dir

    def path(self, dirpath, filename):
        return dirpath + filename

    """
    top dir
    """

    def create_repo(self):
        """Create a repository."""

        def create_conf_file(path):
            config = configparser.ConfigParser()
            config["server_1"] = {
                "host": "localhost",
                "port": "5432",
                "username": "postgres",
                "input_password": "false",
                "password": "",
            }
            config["server_2"] = {}
            with open(path, "w") as configfile:
                config.write(configfile)

        # Check directory.
        if os.path.exists(self.base_dir + self.REPOSITORY_DIR):
            print(
                "Error: directory '{}' already exists.".format(
                    self.base_dir + self.REPOSITORY_DIR
                )
            )
            sys.exit(1)

        # Make repository directory.
        if os.path.exists(self.base_dir) == False:
            os.mkdir(self.base_dir)
        os.mkdir(self.base_dir + self.REPOSITORY_DIR, mode=self.DEFAULT_DIR_MODE)

        # Create hosts.conf file.
        _conf_file_path = self.get_conf_file_path()
        create_conf_file(_conf_file_path)
        os.chmod(_conf_file_path, self.DEFAULT_HOSTS_CONF_MODE)

    def is_serverId_valid(self, serverId):
        return (
            True if re.search(r"\w+", serverId, flags=0).group() == serverId else False
        )

    def check_host_conf_file(self):
        _path = self.get_conf_file_path()
        # Check mode.
        print("Checking hosts.conf mode....")
        if self.secure_check(_path, self.DEFAULT_HOSTS_CONF_MODE) == True:
            print("\tReport: {} is secure.".format(self.CONF_FILE))
        else:
            print(
                "\tError: {}'s mode should be set to {} or more secure.".format(
                    self.CONF_FILE, oct(self.DEFAULT_HOSTS_CONF_MODE)
                )
            )

        # Check serverIds.
        print("Checking serverIds....")
        _config = configparser.ConfigParser()
        _config.read(_path)
        _ret = True
        for s in _config.sections():
            if self.is_serverId_valid(s) == False:
                print("\tError: serverId '{}' is invalid name.".format(s))
                _ret = False
        if _ret == True:
            print("\tReport: All serverIds are valid.")

    def check_dirs(self):
        print("Checking directories....")
        _path = self.base_dir + self.REPOSITORY_DIR + "/"
        _dirlist = os.listdir(_path)
        for _dir in _dirlist:
            _dirpath = _path + _dir
            if os.path.isdir(_dirpath):
                if self.secure_check(_dirpath, self.DEFAULT_DIR_MODE) == True:
                    print("\tReport: {} is secure.".format(_dirpath))
                else:
                    print(
                        "\tError: {}'s mode should be set to {} or more secure.".format(
                            _dirpath, oct(self.DEFAULT_DIR_MODE)
                        )
                    )
                for subdir in (
                    self.TABLES_DIR,
                    self.GROUPING_DIR,
                    self.REGRESSION_DIR,
                    self.FORMATTED_REGRESSION_PARAMS_DIR,
                ):
                    _subdirpath = _dirpath + "/" + subdir
                    if self.secure_check(_subdirpath, self.DEFAULT_DIR_MODE) == True:
                        print("\tReport: {} is secure.".format(_subdirpath))
                    else:
                        print(
                            "\tError: {}'s mode should be set to {} or more secure.".format(
                                _subdirpath, oct(self.DEFAULT_DIR_MODE)
                            )
                        )

    def rename_serverId(self, old_serverId, new_serverId):
        def mv_dir(old_serverId, new_serverId):
            _dirpath = self.base_dir + self.REPOSITORY_DIR + "/"
            os.rename(_dirpath + old_serverId, _dirpath + new_serverId)

        # Check new_serverId.
        if self.is_serverId_valid(new_serverId) == False:
            print("Error: new serverId '{}' is invalid name.".format(new_serverId))
            sys.exit(1)
        # Check old_serverId.
        if self.check_serverId(old_serverId) == False:
            print("Error: old serverId '{}' does not exit.".format(old_serverId))
            sys, exit(1)
        # Change serverId directories.
        mv_dir(old_serverId, new_serverId)
        # Change serverId from old_serverId to new_serverId in the host.conf file.
        _conf_path = self.get_conf_file_path()
        _conf_tmp_path = _conf_path + ".tmp"
        os.rename(_conf_path, _conf_tmp_path)
        try:
            fp_conf = open(_conf_path, mode="w")
            with open(_conf_tmp_path, mode="r") as fp_conf_tmp:
                for _line in fp_conf_tmp:
                    if str("[" + old_serverId + "]") in _line:
                        _line = str("[" + new_serverId + "]" + "\n")
                    fp_conf.write(_line)
            os.remove(_conf_tmp_path)
        except Exception as e:
            os.rename(_conf_tmp_path, _conf_path)
            mv_dir(new_serverId, old_serverId)
            print(e)
            print("Error: Could not rename serverId.")
        finally:
            os.chmod(_conf_path, self.DEFAULT_HOSTS_CONF_MODE)
            fp_conf.close()

    def remove_serverId(self, serverId):
        def rm_dir(serverId):
            _dirpath = self.base_dir + self.REPOSITORY_DIR + "/" + serverId
            if os.path.exists(_dirpath):
                shutil.rmtree(_dirpath)
                if Log.debug1 <= self.LogLevel:
                    print("Debug1: Deleted {}.".format(_dirpath))
                return True
            else:
                print("Debug1: {} Not Found.".format(_dirpath))
                return False

        # Check serverId.
        if self.check_serverId(serverId) == False:
            print("Error: serverId '{}' does not exit.".format(serverId))
            sys, exit(1)
        # Delete serverId directories.
        return rm_dir(serverId)

    def show_hosts(self, verbose):
        _path = self.get_conf_file_path()
        _config = configparser.ConfigParser()
        _config.read(_path)
        print("ServerId:")
        for section in _config.sections():
            if "host" in _config[section]:
                print("\t{}".format(section))
                if verbose == True:
                    print("\t\thost = {}".format(_config[section]["host"]))
                    if "port" in _config[section]:
                        print("\t\tport = {}".format(_config[section]["port"]))
                    if "username" in _config[section]:
                        print("\t\tusername = {}".format(_config[section]["username"]))

    """
    tables subdir
    """

    def update_tables_stat_file(self, serverId, max_seqid):
        self.__update_stat_file(serverId, max_seqid, self.TABLES_DIR)

    def get_seqid_from_tables_stat(self, serverId):
        return self.__get_seqid_from_stat_file(serverId, self.TABLES_DIR)

    def check_tables_dir(self, serverId):
        self.__check_dir(serverId, self.TABLES_DIR, [])

    def reset_tables_dir(self, serverId):
        self.__reset_dir(serverId, self.TABLES_DIR, self.update_tables_stat_file)

    def get_log_csv_path(self, serverId):
        _csvdirpath = self.dirpath([serverId, self.TABLES_DIR])
        return self.path(_csvdirpath, self.TABLES_FILE)

    def get_query_dir_path(self, serverId, queryid):
        return self.dirpath(
            [
                serverId,
                self.TABLES_DIR,
                self.TABLES_QUERY_DIR,
                self.hash_dir(queryid),
                str(queryid),
            ]
        )

    def get_plan_dir_path(self, serverId, queryid, planid):
        return self.dirpath(
            [
                serverId,
                self.TABLES_DIR,
                self.TABLES_PLAN_DIR,
                self.hash_dir(planid),
                str(queryid) + "." + str(planid),
            ]
        )

    def get_plan_json_dir_path(self, serverId, queryid, planid):
        return self.dirpath(
            [
                serverId,
                self.TABLES_DIR,
                self.TABLES_PLAN_JSON_DIR,
                self.hash_dir(planid),
                str(queryid) + "." + str(planid),
            ]
        )

    def get_plan_json_path(self, serverId, seqid, queryid, planid):
        _logdirpath = self.get_plan_json_dir_path(serverId, queryid, planid)
        return self.path(_logdirpath, str(seqid))

    def get_query(self, serverId, queryid):
        """Get Query by queryid"""

        _dirpath = self.dirpath(
            [
                serverId,
                self.TABLES_DIR,
                self.TABLES_QUERY_DIR,
                self.hash_dir(int(queryid)),
                str(queryid),
            ]
        )
        _files = glob.glob(_dirpath + "[0-9]*")
        for _qf in _files:
            _seqid_file = _qf.split("/")[-1]
            # Get query
            with open(_qf) as fp:
                _query = fp.read()

            # Get database from log.csv
            with open(self.get_log_csv_path(self.ServerId), newline="") as f:
                _reader = csv.reader(f, delimiter=",", quoting=csv.QUOTE_NONE)
                for _row in _reader:
                    _seqid = int(_row[0])
                    _database = str(_row[3])
                    _planid = int(_row[7])
                    if int(_seqid_file) == _seqid:
                        return (_database, _query, _planid)
        return (None, None, None)

    """
    grouping subdir
    """

    def update_grouping_stat_file(self, serverId, max_seqid):
        self.__update_stat_file(serverId, max_seqid, self.GROUPING_DIR)

    def get_seqid_from_grouping_stat(self, serverId):
        return self.__get_seqid_from_stat_file(serverId, self.GROUPING_DIR)

    def check_grouping_dir(self, serverId):
        self.__check_dir(serverId, self.GROUPING_DIR, [])

    def reset_grouping_dir(self, serverId):
        self.__reset_dir(serverId, self.GROUPING_DIR, self.update_grouping_stat_file)

    def get_grouping_plan_dir_path(self, serverId, planid):
        return self.dirpath([str(serverId), self.GROUPING_DIR, self.hash_dir(planid)])

    def get_grouping_plan_path(self, serverId, queryid, planid):
        return self.path(
            self.get_grouping_plan_dir_path(serverId, planid),
            str(queryid) + "." + str(planid),
        )

    def get_grouping_dir_path(self, serverId):
        return self.dirpath([serverId, self.GROUPING_DIR])

    def get_grouping_dir_list(self, serverId):
        return os.listdir(self.dirpath([serverId, self.GROUPING_DIR]))

    def get_grouping_subdir_path(self, serverId, subdir):
        return self.dirpath([serverId, self.GROUPING_DIR, subdir])

    def get_grouping_subdir_list(self, serverId, subdir):
        return os.listdir(self.dirpath([serverId, self.GROUPING_DIR, subdir]))

    """
    regression subdir
    """

    def update_regression_stat_file(self, serverId, max_seqid):
        self.__update_stat_file(serverId, max_seqid, self.REGRESSION_DIR)

    def get_seqid_from_regression_stat(self, serverId):
        return self.__get_seqid_from_stat_file(serverId, self.REGRESSION_DIR)

    def check_regression_dir(self, serverId):
        self.__check_dir(serverId, self.REGRESSION_DIR, [])

    def reset_regression_dir(self, serverId):
        self.__reset_dir(
            serverId, self.REGRESSION_DIR, self.update_regression_stat_file
        )

    def get_regression_subdir_path(self, serverId, subdir):
        return self.dirpath([serverId, self.REGRESSION_DIR, subdir])

    def get_regression_param(self, serverId, queryid, planid):
        _key = str(queryid) + "." + str(planid)
        _pathdir = self.dirpath([serverId, self.REGRESSION_DIR, self.hash_dir(planid)])
        _path = self.path(_pathdir, _key)
        if os.path.exists(_path):
            return self.read_plan_json(_path)
        else:
            return None

    """
    formatted regression parameter subdir
    """

    def check_formatted_regression_params_dir(self, serverId):
        self.__check_dir(serverId, self.FORMATTED_REGRESSION_PARAMS_DIR, [])

    def get_formatted_regression_params_subdir_path(self, serverId):
        return self.dirpath([serverId, self.FORMATTED_REGRESSION_PARAMS_DIR])

    def truncate_formatted_regression_params(self, serverId):
        _dir = self.get_formatted_regression_params_subdir_path(serverId)
        for _file_name in os.listdir(_dir):
            os.remove(str(_dir) + "/" + str(_file_name))

    def write_formatted_regression_params(self, serverId, queryid, param):
        _dir = self.get_formatted_regression_params_subdir_path(serverId)
        with open(str(_dir) + "/" + str(queryid), mode="w") as _fp:
            _fp.write(param)

    def check_formatted_regression_params(self, serverId, queryid):
        _dir = self.get_formatted_regression_params_subdir_path(serverId)
        if os.path.exists(_dir) == False:
            # Formatted regression params has not been created yet.
            # These params are created when push command is issued.
            return False
        for _file in os.listdir(_dir):
            if str(_file) == str(queryid):
                return True
        return False
