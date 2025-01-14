"""
common.py

  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 2021-2025, Hironobu Suzuki @ interdb.jp
"""

import hashlib
import json
import os
import sys
import configparser

from enum import Enum
from enum import IntEnum

"""
Helper classes
"""


class State(Enum):
    WAITING = 0  # This node is not running.
    RUNNING = 1  # This node is running.
    FINISHED = 2  # This node has finished processing at least once.


class Log(IntEnum):
    error = 0
    warning = 1
    notice = 2
    info = 3
    debug1 = 4
    debug2 = 5
    debug3 = 6
    debug4 = 7
    debug5 = 8


"""
Common
"""


class Common:
    def __init__(self):
        pass

    """
    Global variables
    """

    """top directory"""
    REPOSITORY_DIR = "pgpi_repository"
    CONF_FILE = "hosts.conf"
    STAT_FILE = "stat.dat"

    """tables directory"""
    TABLES_DIR = "tables"
    TABLES_FILE = "log.csv"
    TABLES_QUERY_DIR = "query"
    TABLES_PLAN_DIR = "plan"
    TABLES_PLAN_JSON_DIR = "plan_json"

    """grouping directory"""
    GROUPING_DIR = "grouping"

    """regression directory"""
    REGRESSION_DIR = "regression"

    """formatted regression parameter directory"""
    FORMATTED_REGRESSION_PARAMS_DIR = "reg_params"

    """pg_query_plan"""
    SCHEMA = "query_plan"
    LOG_TABLE = "log"

    REG_PARAMS_TABLE = "reg"

    """
    Various methods
    """

    def hash_dir(self, num):
        return str(num % 1000).zfill(3)

    def input_serverId(self):
        _msg = "Enter serverId:"
        try:
            _serverId = input(_msg)
        except KeyboardInterrupt:
            print("\nInput Interrupted.\n")
            sys.exit(1)
        return _serverId

    def apply_func_in_each_node(self, func, Plans):
        if isinstance(Plans, list):
            for plan in Plans:
                plan = func(plan)
                if "Plans" in plan:
                    self.apply_func_in_each_node(func, plan["Plans"])
            return
        else:
            Plans = func(Plans)
            if "Plan" in Plans:
                self.apply_func_in_each_node(func, Plans["Plan"])
            if "Plans" in Plans:
                self.apply_func_in_each_node(func, Plans["Plans"])
            return

    def delete_unnecessary_objects(self, delete_objects_func, Plans):
        self.apply_func_in_each_node(delete_objects_func, Plans)

    def count_workers(self, Plans):
        """Count the numbers of Planned Workers and Launched Workers."""

        def count(plan):
            if "Workers Planned" in plan:
                return (plan["Workers Planned"] + 1, plan["Workers Launched"] + 1)
            else:
                return None

        if isinstance(Plans, list):
            for plan in Plans:
                _ret = count(plan)
                if _ret is not None:
                    return _ret
                if "Plans" in plan:
                    _ret = self.count_workers(plan["Plans"])
            return _ret
        else:
            _ret = count(Plans)
            if _ret is not None:
                return _ret
            if "Plan" in Plans:
                _ret = self.count_workers(Plans["Plan"])
            if "Plans" in Plans:
                _ret = self.count_workers(Plans["Plans"])
            return _ret

    def count_nodes(self, Plans):
        """
        Count the number of nodes in Plans.
        """

        def count_nodes(Plans):
            if isinstance(Plans, list):
                for plan in Plans:
                    count(plan)
                    if "Plans" in plan:
                        count_nodes(plan["Plans"])
                return
            else:
                count(Plans)
                if "Plan" in Plans:
                    count_nodes(Plans["Plan"])
                if "Plans" in Plans:
                    count_nodes(Plans["Plans"])
                return

        def count(plan):
            if "Node Type" in plan:
                self.__num += 1

        self.__num = 0
        count_nodes(Plans)
        return self.__num

    def read_plan_json(self, planpath):
        """Read the plan from planpath."""
        _js = open(planpath, "r")
        _json_dict = json.load(_js)
        _js.close()
        return _json_dict

    def write_plan_json(self, jdict, planpath):
        """Write the plan (jdict) to planpath."""

        _jdp = json.dumps(jdict, ensure_ascii=False, indent=4, separators=(",", ": "))
        _fp = open(planpath, "w")
        _fp.write("{}".format(_jdp))
        _fp.close()

    def isScan(self, plan):
        """Check this plan is scan or not."""
        for _i in (
            "Result",
            "Seq Scan",
            "Sample Scan",
            "Index Scan",
            "Index Only Scan",
            "Bitmap Index Scan",
            "Bitmap Heap Scan",
            "Tid Scan",
            "Function Scan",
            "Table Function Scan",
            "Values Scan",
            "CTE Scan",
            "Named Tuplestore Scan",
            "WorkTable Scan",
        ):  # 'Foreign Scan', 'Aggregate', 'SetOp', 'Limit'
            if _i in plan["Node Type"]:
                return True
        return False

    def isOuter(self, plan):
        """Check this plan is outer path or not."""
        if "Parent Relationship" in plan:
            return True if plan["Parent Relationship"] == "Outer" else False
        return False

    def isInner(self, plan):
        """Check this plan is inner path or not."""
        if "Parent Relationship" in plan:
            return True if plan["Parent Relationship"] == "Inner" else False
        return False

    def count_removed_rows(self, plan):
        """Count Removed Rows."""
        _rr = 0
        for _i in (
            "Rows Removed by Filter",
            "Rows Removed by Index Recheck",
            "Rows Removed by Join Filter",
            "Rows Removed by Conflict Filter",
        ):
            if _i in plan:
                _rr += plan[_i]
        return _rr

    def get_inputs(self, plan):
        """Get outer and inter actual rows."""

        def get_removed_rows(plan, num):
            _rr = []
            for r in (
                "Rows Removed by Filter",
                "Rows Removed by Index Recheck",
                "Rows Removed by Join Filter",
                "Rows Removed by Conflict Filter",
            ):
                if r in plan:
                    _rr = plan[r]
                if len(_rr) == 0:
                    _rr = [0] * num
            return _rr

        def get_children_actual_rows(plan):
            _X = [[], []]
            for i in range(0, 2):  # Ignore SubPlans
                p = plan["Plans"][i]
                k = 0 if p["Parent Relationship"] == "Outer" else 1
                _X[k] += p["Actual Rows"]
            return (_X[0], _X[1])

        (_Xouter, _Xinner) = get_children_actual_rows(plan)
        _RR = get_removed_rows(plan, len(_Xouter))

        return (_Xouter, _Xinner, _RR)
