#!/usr/bin/env python3
"""
analyze.py

Usage:
    analyze.py [--basedir XXX] ServerId


  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 2021-2025, Hironobu Suzuki @ interdb.jp
"""

import argparse
import sys
import os
import re
import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from pgpi import *


class NN:
    def __init__(self, base_dir=".", log_level=Log.error):
        self.ServerId = ""
        self.LogLevel = log_level

    """For Neural Net"""
    NN_DIR = "nn"
    NN_THRESHOLD = 3

    def store_params(
        self, serverId, queryid, planid, depth, node_type, xouter, xinner, y
    ):
        _keys = ["Depth", "Node Type", "Xouter", "Xinner", "Y"]
        _values = [depth, node_type, xouter, xinner, y]
        _dict = dict(zip(_keys, _values))

        _pathdir = self.NN_DIR + "/" + serverId + "/"
        if os.path.exists(_pathdir) == False:
            os.makedirs(_pathdir, self.DEFAULT_DIR_MODE)
        _path = self.path(_pathdir, str(queryid) + "." + str(planid) + "." + str(depth))
        self.write_plan_json(_dict, _path)


class ExtendedStatistics:
    def __init__(self, base_dir=".", log_level=Log.error):
        self.ServerId = ""
        self.LogLevel = log_level

    ES_FILE_PREFIX = "es_"
    ES_THRESHOLD = 10
    COND_LIST = (
        "Index Cond",
        "Recheck Cond",
        "TID Cond",
        "Merge Cond",
        "Hash Cond",
        "Filter",
        "Join Filter",
    )

    def __check_conds(self, conds):
        _ret = []
        for _cond in conds:
            if re.search(r">|<", _cond) == None:
                _ret.append(_cond)
        return list(set(_ret)) if len(_ret) > 0 else None

    def check_es(self, plan, queryid, planid, depth):
        def check_conds(plan):
            _ret = None
            for n in self.COND_LIST:
                if n in plan:
                    _ret = self.__check_conds(plan[n])
            return _ret

        _ret = None
        for i in range(0, len(plan["Actual Rows"])):
            _actual_rows = plan["Actual Rows"][i]
            _plan_rows = plan["Plan Rows"][i]
            if (
                _actual_rows * self.ES_THRESHOLD < _plan_rows
                or _plan_rows * self.ES_THRESHOLD < _actual_rows
            ) and (0 < _actual_rows and 0 < _plan_rows):

                _ret = check_conds(plan)
                if _ret != None:
                    return _ret

                if "Plans" in plan:
                    if isinstance(plan, list):
                        for k in range(0, 2):  # Ignore SubPlans
                            _ret = check_conds(plan["Plans"][k])
                            if _ret != None:
                                return _ret
                    else:
                        _ret = check_conds(plan["Plans"][0])
                        if _ret != None:
                            return _ret
        return _ret


class Histogram:
    def __init__(self, base_dir=".", log_level=Log.error):
        self.ServerId = ""
        self.LogLevel = log_level

    HIST_FILE_PREFIX = "hist_"
    HIST_THRESHOLD = 0.025
    DIFF_THRESHOLD = 0.001
    RANGE_THRESHOLD = 1.75
    COND_LIST = (
        "Index Cond",
        "Recheck Cond",
        "TID Cond",
        "Merge Cond",
        "Hash Cond",
        "Filter",
        "Join Filter",
    )

    def __get_bounds(self, y):
        return (int((1 - self.HIST_THRESHOLD) * y), int((1 + self.HIST_THRESHOLD) * y))

    def __min_max(self, y):
        _max = 0
        for i in range(0, len(y)):
            if _max < y[i]:
                _max = y[i]
        _min = _max
        for i in range(0, len(y)):
            if y[i] < _min:
                _min = y[i]
        return (_min, _max)

    def check_histogram(self, plan, queryid, planid, depth):
        def check_conds(conds):
            _ret = []
            for _cond in conds:
                _ret.append(_cond)
            return list(set(_ret)) if len(_ret) > 0 else None

        def create_item(_dict_list, y, x, _lower, _upper):
            _d = {
                "y": [y],
                "y_lower": _lower,
                "y_upper": _upper,
                "x": [x],
                "x_lower": x,
                "x_upper": x,
            }
            _dict_list.append(_d)

        def append_data(_dict_list, y, x):
            (_lower, _upper) = self.__get_bounds(y)
            if len(_dict_list) == 0:
                create_item(_dict_list, y, x, _lower, _upper)
                return
            else:
                for _dict in _dict_list:
                    if _dict["y_lower"] <= y and y <= _dict["y_upper"]:
                        _dict["y"].append(y)
                        _dict["x"].append(x)
                        if x < _dict["x_lower"]:
                            _dict["x_lower"] = x
                        if _dict["x_upper"] < x:
                            _dict["x_upper"] = x
                        return
            # Add new key-values.
            create_item(_dict_list, y, x, _lower, _upper)
            return

        _dict_list = []
        _ret = None
        if "Plan Rows" in plan:
            _plan_rows = plan["Plan Rows"]
            _actual_rows = plan["Actual Rows"]

            for i in range(0, len(_plan_rows)):
                append_data(_dict_list, _plan_rows[i], _actual_rows[i])

            for i in range(0, len(_dict_list)):
                _dict = _dict_list[i]
                _y = _dict["y"]
                _x = _dict["x"]
                if len(_y) > 1:
                    (_y_lower, _y_upper) = self.__min_max(_y)
                    _x_lower = _dict["x_lower"]
                    _x_upper = _dict["x_upper"]
                    _d_y = _y_upper - _y_lower
                    _d_x = _x_upper - _x_lower

                    if _x_upper <= _x_lower * self.RANGE_THRESHOLD or _d_x == 0:
                        continue
                    if _d_y / _d_x < self.DIFF_THRESHOLD:
                        for n in self.COND_LIST:
                            if n in plan:
                                _ret = check_conds(plan[n])

        return _ret


class Analyze(Repository, ExtendedStatistics, NN, Histogram):
    def __init__(self, base_dir=".", log_level=Log.error):
        self.ServerId = ""
        self.Level = 0
        self.set_base_dir(base_dir)
        self.LogLevel = log_level

    def __set_serverId(self, serverId):
        self.ServerId = serverId

    def set_log_level(self, log_level):
        self.LogLevel = log_level

    """
    Handle self.Level value.
    """

    def __init_level(self):
        self.Level = 0

    def __incr_level(self):
        self.Level += 1

    def __get_level(self):
        return self.Level

    def __calc(self, plan, queryid, planid, depth):

        self.__incr_level()
        _level = self.__get_level()

        _node_type = plan["Node Type"]
        """
        nested loop type
        """
        for n in (
            "Append",
            "Merge Append",
            "Recursive Union",
            "Nested Loop",
            "BitmapAnd",
            "BitmapOr",
        ):
            if n == _node_type:
                (
                    _Xouter,
                    _Xinner,
                    _RR,
                ) = self.get_inputs(plan)

                # Store parameters.
                _Y = plan["Actual Rows"]
                if len(_Y) >= self.NN_THRESHOLD:
                    self.store_params(
                        self.ServerId,
                        queryid,
                        planid,
                        depth,
                        n,  # "Node Type"
                        _Xouter,
                        _Xinner,
                        _Y,
                    )

                return

        """
        hash or merge join
        """
        for n in ("Merge Join", "Hash Join"):
            if n == _node_type:
                (
                    _Xouter,
                    _Xinner,
                    _RR,
                ) = self.get_inputs(plan)

                # Store parameters.
                _Y = plan["Actual Rows"]
                if len(_Y) >= self.NN_THRESHOLD:
                    self.store_params(
                        self.ServerId,
                        queryid,
                        planid,
                        depth,
                        n,  # "Node Type"
                        _Xouter,
                        _Xinner,
                        _Y,
                    )

                return

        """
        scan type
        """
        (_database, _query, _planid) = self.get_query(self.ServerId, queryid)

        # Check extended statistics.
        _ret = self.check_es(plan, queryid, planid, depth)
        if _ret != None:
            self.__write_data(self.ES_FILE_PREFIX, _database, queryid, _ret, _query)
        # Check statistics' histogram.
        _ret = self.check_histogram(plan, queryid, planid, depth)
        if _ret != None:
            self.__write_data(self.HIST_FILE_PREFIX, _database, queryid, _ret, _query)
        return

    def __get_file_name(self, prefix):
        return prefix + str(self.ServerId) + ".dat"

    def __write_data(self, prefix, database, queryid, ret, query):
        _str = "Candidate:\ndatabase=" + str(database)
        _str += "\nqueryid=" + str(queryid)
        _str += "\nCondition:" + str(ret)
        _str += "\nquery:" + str(query)
        _str += "\n\n"
        _file = self.__get_file_name(prefix)
        with open(_file, mode="a") as f:
            f.write(_str)

    def __analyze(self, Plans, queryid, planid):
        """

        Parameters
        ----------
        Plans : dict
          A plan grouped with the same queryid-planid.
        queryid : int
        planid : int
        """

        def incr(plan):
            if "Node Type" in plan:
                self._count += 1

        def op(Plans, queryid, planid):
            if isinstance(Plans, list):
                for i in range(0, len(Plans)):
                    incr(Plans[i])
                    self.__calc(Plans[i], queryid, planid, self._count)
                    if "Plans" in Plans[i]:
                        op(Plans[i]["Plans"], queryid, planid)
                return
            else:
                incr(Plans)
                self.__calc(Plans, queryid, planid, self._count)
                if "Plans" in Plans:
                    op(Plans["Plans"], queryid, planid)
                return

        # Main procedure.
        self._count = 0
        op(Plans, queryid, planid)

    """
    Public method
    """

    def analyze(self, serverId, command="all"):

        if self.check_serverId(serverId) == False:
            if Log.error <= self.LogLevel:
                print("Error: serverId '{}' is not registered.".format(serverId))
            sys.exit(1)

        self.__set_serverId(serverId)
        self.set_log_level(self.LogLevel)

        # Initialize files.
        for _prefix in (self.ES_FILE_PREFIX, self.HIST_FILE_PREFIX):
            _file = self.__get_file_name(_prefix)
            if os.path.exists(_file):
                if Log.info <= self.LogLevel:
                    print("Info: Remove '{}'".format(_file))
                os.remove(_file)
            if Log.info <= self.LogLevel:
                print("Info: Create '{}'".format(_file))
            with open(_file, "w"):
                pass

        for _hash_subdir in self.get_grouping_dir_list(self.ServerId):
            _gsdirpath = self.get_grouping_subdir_path(self.ServerId, _hash_subdir)
            if os.path.isdir(_gsdirpath):
                for f in self.get_grouping_subdir_list(self.ServerId, _hash_subdir):
                    _gpath = self.path(_gsdirpath, f)
                    _qp_id = str(f).split(".")
                    _queryid = _qp_id[0]
                    _planid = _qp_id[1]

                    _json_dict = self.read_plan_json(_gpath)

                    # Calculate regression parameters in each plan and Store into _reg_param.
                    self.__init_level()
                    self.__analyze(_json_dict["Plan"], _queryid, _planid)


if __name__ == "__main__":

    LOG_LEVEL = Log.info

    # Parse arguments.
    parser = argparse.ArgumentParser(
        description="**** TODO *****",
        add_help=False,
    )
    parser.add_argument(
        "serverid",
        help="Identifier of the database server you connect, which is described in hosts.conf",
        default=None,
    )
    parser.add_argument(
        "--basedir",
        help="Directory of the repository you use (default: '.')",
        default=".",
    )
    parser.add_argument(
        "--command",
        help="Reserved option (currently not used)",
        default="all",
    )
    parser._add_action(
        argparse._HelpAction(
            option_strings=["--help", "-h"], help="Show this help message and exit"
        )
    )

    args = parser.parse_args()

    # Set parameters.
    base_dir = str(args.basedir)
    command = str(args.command).lower()
    if args.serverid is not None:
        server_id = str(args.serverid)
    else:
        print("Error: serverid is not set.")
        sys.exit(-1)

    #
    an = Analyze(base_dir, LOG_LEVEL)
    an.analyze(server_id)
    del an
