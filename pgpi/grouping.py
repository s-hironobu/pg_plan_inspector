"""
grouping.py


  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 2021, Hironobu Suzuki @ interdb.jp
"""

import csv
import os
import sys

from .common import Common, Log
from .repository import Repository


class Grouping(Repository):
    def __init__(self, base_dir=".", log_level=Log.info):
        self.ServerId = ""
        self.is_parallel = False
        self.numWorkers = 1
        self.numPlanWorkers = 1
        self.set_base_dir(base_dir)
        self.LogLevel = log_level

    def __set_serverId(self, serverId):
        self.ServerId = serverId

    UNNECESSARY_OBJECTS = (
        "I/O Read Time",
        "I/O Write Time",
        "Planning Time",
        "Execution Time",
        "Actual Startup Time",
        "Actual Total Time",
        "Time",
        "Actual duration Time",
        "BufferUsage_Start",
        "WalUsage_Start",
        "BufferUsage",
        "WalUsage",
        "Triggers",
        "JIT",
    )

    GROUPING_OBJECTS = (
        "Workers",
        "NormalizePlanParam",
        "NormalizeParam",
        "Plan Rows",
        "Actual Rows",
        "Actual Loops",
        "Rows Removed by Filter",
        "Rows Removed by Index Recheck",
        "Rows Removed by Join Filter",
        "Rows Removed by Conflict Filter",
        "Workers Planned",
        "Workers Launched",
        "Worker",
        "Worker Number",
        "Heap Fetches",
        "Conflicting Tuples",
        "Tuples Inserted",
        "Group Count",
        "Startup Cost",
        "Total Cost",
        "Plan Width",
        "Shared Hit Blocks",
        "Shared Read Blocks",
        "Shared Dirtied Blocks",
        "Shared Written Blocks",
        "Local Hit Blocks",
        "Local Read Blocks",
        "Local Dirtied Blocks",
        "Local Written Blocks",
        "Temp Read Blocks",
        "Temp Written Blocks",
        "Sort Space Used",
        "Sort Space Type",
        "Peak Memory Usage",
        "Original Hash Batches",
        "Original Hash Buckets",
        "Hash Batches",
        "Hash Buckets",
        "Sort Methods Used",
        "Sort Space Memory",
        "Average Sort Space Used",
        "Peak Sort Space Used",
        "Exact Heap Blocks",
        "Lossy Heap Blocks",
        "Function Call",
        "Calls",
        "Target Tables",
        "Conflict Resolution",
        "Conflict Arbiter Indexes",
        "Sampling Method",
        "Sampling Parameters",
        "Repeatable Seed",
        "Table Function Name",
        "Presorted Key",
        "Full-sort Groups",
        "Pre-sorted Groups",
        "Subplans Removed",
        "Index Cond",
        "Recheck Cond",
        "TID Cond",
        "Merge Cond",
        "Hash Cond",
        "Filter",
        "Join Filter",
    )

    def __delete_objects(self, node):
        """Delete unnecessary objects from node."""

        for _i in self.UNNECESSARY_OBJECTS:
            if _i in node:
                del node[_i]
        return node

    def __convert_to_list(self, Plans):
        """Covert to the list type in order to append the object."""

        def to_list(plan):
            for _go in self.GROUPING_OBJECTS:
                if _go in plan:
                    plan.update({_go: [plan[_go]]})

        if isinstance(Plans, list):
            for _plan in Plans:
                to_list(_plan)
                if "Plans" in _plan:
                    self.__convert_to_list(_plan["Plans"])
            return
        else:
            to_list(Plans)
            if "Plan" in Plans:
                self.__convert_to_list(Plans["Plan"])
            if "Plans" in Plans:
                self.__convert_to_list(Plans["Plans"])
            return

    def __append_objects(self, target_Plans, Plans):
        def append_value_to_list(targetplan, plan):
            for _go in self.GROUPING_OBJECTS:
                if _go in targetplan:
                    targetplan[_go] += plan[_go]

        if isinstance(target_Plans, list):
            for _i in range(0, len(target_Plans)):
                append_value_to_list(target_Plans[_i], Plans[_i])
                if "Plans" in target_Plans[_i]:
                    self.__append_objects(target_Plans[_i]["Plans"], Plans[_i]["Plans"])
            return
        else:
            append_value_to_list(target_Plans, Plans)
            if "Plan" in target_Plans:
                self.__append_objects(target_Plans["Plan"], Plans["Plan"])
            if "Plans" in target_Plans:
                self.__append_objects(target_Plans["Plans"], Plans["Plans"])
            return

    def __combine_plan(self, planpath, logpath):
        """Combine the plan (planpath) with the combined plan (logpath)."""

        _json_dict = self.read_plan_json(logpath)
        self.delete_unnecessary_objects(self.__delete_objects, _json_dict)
        self.__convert_to_list(_json_dict)
        if os.path.exists(planpath):
            _target_json_dict = self.read_plan_json(planpath)
            self.__append_objects(_target_json_dict, _json_dict)
            self.write_plan_json(_target_json_dict, planpath)
        else:
            self.write_plan_json(_json_dict, planpath)

    """
    Public method
    """

    def grouping(self, serverId):
        """
        Combine the json plans with the same queryId+planId, which are stored
        in the Tables directory, into one json plan, and store it under the
        Grouping directory.
        """

        if self.check_serverId(serverId) == False:
            if Log.error <= self.LogLevel:
                print("Error: serverId '{}' is not registered.".format(serverId))
            sys.exit(1)

        self.__set_serverId(serverId)

        if Log.info <= self.LogLevel:
            print("Info: Grouping json formated plans.")

        """Get max_seqid from tables stat."""
        _max_seqid = self.get_seqid_from_tables_stat(self.ServerId)

        """Check the grouping dir and get the current seqid."""
        self.check_grouping_dir(self.ServerId)
        _current_seqid = self.get_seqid_from_grouping_stat(self.ServerId)

        if _current_seqid >= _max_seqid:
            return

        """
        Read log.csv to get the queryid and planid between current_seqid
        and max_seqid.
        """
        with open(self.get_log_csv_path(self.ServerId), newline="") as f:
            _reader = csv.reader(f, delimiter=",", quoting=csv.QUOTE_NONE)

            for _row in _reader:
                _seqid = int(_row[0])
                _queryid = int(_row[6])
                _planid = int(_row[7])

                if _current_seqid < _seqid and _seqid <= _max_seqid:
                    """
                    Get the path of the combined plan (_queryid and _planid),
                    stored in the Grouping dir.
                    """
                    _logpath = self.get_plan_json_path(
                        self.ServerId, _seqid, _queryid, _planid
                    )

                    if os.path.isfile(_logpath) == False:
                        if Log.debug1 <= self.LogLevel:
                            print("Debug1: seqid({}) is not found.)".format(_seqid))
                        continue
                    """
                    Get the path of the plan (_queryid and _planid) that is stored
                    in the Tables dir.
                    """
                    _plandirpath = self.get_grouping_plan_dir_path(
                        self.ServerId, _planid
                    )
                    _planpath = self.get_grouping_plan_path(
                        self.ServerId, _queryid, _planid
                    )
                    if os.path.exists(_plandirpath) == False:
                        os.mkdir(_plandirpath)
                    """
                    Combine the plan (_planpath) with the combined plan (_logpath).
                    """
                    self.__combine_plan(_planpath, _logpath)
                    if Log.debug3 <= self.LogLevel:
                        print("Debug3: planpath={}".format(_planpath))
                        print("Debug3:    logpath={}".format(_logpath))

            """Update grouping/stat.dat."""
            self.update_grouping_stat_file(self.ServerId, _max_seqid)
