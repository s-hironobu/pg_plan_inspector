"""
merge_plan.py

This file defines a set of classes to adjust the effects of parallel workers.


  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 2021, Hironobu Suzuki @ interdb.jp
"""

import csv
import glob
import operator
import os
from .common import Log
from .repository import Repository


class PrepareMergeRows:
    def __init__(self, base_dir=".", log_level=Log.info):
        self.set_base_dir(base_dir)
        self.LogLevel = log_level

    def __init_val(self):
        self.is_parallel = False  # Whether this plan node is working with parallel.
        self.numWorkers = 1
        self.numPlanWorkers = 1

    def __add_helper_objects(self, Plans, is_outer):
        """
        Add three helper objects in each node to prepare
        to execute AddRows.__add_rows().
        """

        def add_helper_object(plan, is_outer):
            if "Workers Planned" in plan:
                self.is_parallel = True
                if isinstance(plan["Workers Planned"], list):
                    self.numPlanWorkers += plan["Workers Planned"][0]
                else:
                    self.numPlanWorkers += plan["Workers Planned"]
            if "Workers Launched" in plan:
                if isinstance(plan["Workers Planned"], list):
                    self.numWorkers += plan["Workers Launched"][0]
                else:
                    self.numWorkers += plan["Workers Launched"]

            if "Actual Rows" in plan:
                """
                To adjust the effect of parallel workers, the "NormalizeParam"
                and "NormalizePlanParam" objects are added in each node.
                Also, one object "MergeFlag" is added to set whether the node
                needs to adjust the effect of parallel workers or not.

                If `plan` is working in parallel and if the node and all the nodes
                connected to this node are outer, the values "Plan Rows" should be
                adjusted by both numbers of "plan workers" and "launched workers".

                An example is shown below.

                Example:
                Finalize Aggregate                   <-- No Need to adjust
                  ->  Gather                         <-- No Need to adjust
                    ->  Partial Aggregate            <-- Need to adjust
                       ->  Nested Loop               <-- Need to adjust
                          ->  Nested Loop            <-- Need to adjust
                               ->  Parallel Seq Scan <-- Need to adjust
                               ->  Index Only Scan   <-- No Need
                          ->  Seq Scan               <-- No Need

                The numbers of the plan workers and launched workers are stored in
                the "NormalizeParam" and "NormalyzePlanParm" objects in each node,
                respectively, and these are used when calculating the regression
                parameters, merging plans, and so on.
                """
                if self.is_parallel and is_outer:
                    if plan["Node Type"] == "Aggregate":
                        if Log.debug3 <= self.LogLevel:
                            print(
                                "Debug3: NodeType={}  MergeFlag = False".format(
                                    plan["Node Type"]
                                )
                            )
                        plan.update({"MergeFlag": "False"})
                        plan.update({"NormalizeParam": 1})
                        plan.update({"NormalizePlanParam": 1})
                    else:
                        if Log.debug3 <= self.LogLevel:
                            print(
                                "Debug3: NodeType={}  MergeFlag = True".format(
                                    plan["Node Type"]
                                )
                            )
                        plan.update({"MergeFlag": "True"})
                        plan.update({"NormalizeParam": self.numWorkers})
                        plan.update({"NormalizePlanParam": self.numPlanWorkers})
                else:
                    if Log.debug3 <= self.LogLevel:
                        print(
                            "Debug3: NodeType={}  MergeFlag = False".format(
                                plan["Node Type"]
                            )
                        )
                    plan.update({"MergeFlag": "False"})
                    plan.update({"NormalizeParam": 1})
                    plan.update({"NormalizePlanParam": 1})

        if isinstance(Plans, list):
            for _i in range(0, len(Plans)):
                plan = Plans[_i]
                """
                If this node is working in parallel but is not outer
                (i: 0 = outer, 1 = inner), set False to is_parallel.
                """
                if self.is_parallel and _i != 0:
                    is_outer = False
                add_helper_object(plan, is_outer)
                if "Plans" in plan:
                    self.__add_helper_objects(plan["Plans"], is_outer)
            return
        else:
            add_helper_object(Plans, is_outer)
            if "Plan" in Plans:
                self.__add_helper_objects(Plans["Plan"], is_outer)
            if "Plans" in Plans:
                self.__add_helper_objects(Plans["Plans"], is_outer)
            return

    """
    Public methods
    """

    def prepare_merge_rows(self, Plans):
        """
        Add three helper objects in each node to prepare to execute
        AddRows.__add_rows(), and Return numbers of workers of both
        planned and launched.
        """
        self.__init_val()
        self.__add_helper_objects(Plans, True)
        return (self.numPlanWorkers, self.numWorkers)

    def delete_flags(self, plan):
        """
        Use with delete_unnecessary_objects().
        """
        if "MergeFlag" in plan:
            del plan["MergeFlag"]
        return plan


class MergeRows(Repository, PrepareMergeRows):
    def __init__(self, log_level=Log.info):
        self.ServerId = ""
        self.LogLevel = log_level

    def __set_serverId(self, serverId):
        self.ServerId = serverId

    def __get_actual_rows(self, Plans, depth):
        """
        Return the "Actual Rows" of the node whose depth is `depth` of `Plans`.

        Parameters
        ----------
        Plans : dict
        depth : int

        Returns
        -------
        _rows : int
        """

        def incr(plan):
            if "Node Type" in plan:
                self._count += 1

        def op(Plans, depth):
            if isinstance(Plans, list):
                for plan in Plans:
                    incr(plan)
                    if depth == self._count:
                        self._rows = plan["Actual Rows"]
                        return
                    if "Plans" in plan:
                        op(plan["Plans"], depth)
                return
            else:
                incr(Plans)
                if depth == self._count:
                    self._rows = Plans["Actual Rows"]
                    return
                if "Plan" in Plans:
                    op(Plans["Plan"], depth)
                if "Plans" in Plans:
                    op(Plans["Plans"], depth)
                return

        # Main procedure.
        self._count = 0
        self._rows = 0
        op(Plans, depth)
        return self._rows

    def __add_rows(self, Plans, rows, depth):
        """
        Add `rows` to "Actual Rows" of the node whose depth is `depth`,
        and Adjust "Plan Rows" using "NormalizePlanParam" if necessary.

        Parameters
        ----------
        Plans : dict
        rows : int
        depth : int
        """

        def incr(plan):
            if "Node Type" in plan:
                self._count += 1

        def add(plan, rows):
            if "MergeFlag" in plan:
                if plan["MergeFlag"] == "True":
                    plan["Plan Rows"] *= plan["NormalizePlanParam"]
                    plan["Actual Rows"] += rows

        def op(Plans, rows, depth):
            if isinstance(Plans, list):
                for plan in Plans:
                    incr(plan)
                    if depth == self._count:
                        add(plan, rows)
                        return
                    elif "Plans" in plan:
                        op(plan["Plans"], rows, depth)
                return
            else:
                incr(Plans)
                if depth == self._count:
                    add(Plans, rows)
                    return
                if "Plan" in Plans:
                    op(Plans["Plan"], rows, depth)
                elif "Plans" in Plans:
                    op(Plans["Plans"], rows, depth)
                return

        # Main procedure.
        self._count = 0
        op(Plans, rows, depth)

    """
    Public methods
    """

    def merge_rows(self, leader_plan, worker_plans):
        """
        Merge the 'Plan Rows' and 'Actual Rows' of all parallel worker'
        plans(workerplans) into the leader's plan(leader_plan) if necessary.

        Parameters
        ----------
        leader_plan : dict
        worker_plans : [dict, ...]
        """
        _num_worker_node = self.count_nodes(worker_plans[0])
        _num_leader_node = self.count_nodes(leader_plan)
        _i = _num_worker_node
        _j = _num_leader_node
        while 0 < _i:
            rows = 0
            # Add all parallel workers' "Actual Rows" to `rows`.
            for _k in range(0, len(worker_plans)):
                rows += self.__get_actual_rows(worker_plans[_k], _i)
            # Adjust "Plan Rows" and "Actual Rows" of `leader_plan`.
            self.__add_rows(leader_plan, rows, _j)
            _i -= 1
            _j -= 1

    def extrapolate_rows(self, leader_plan, num_actual_workers, num_workers):
        """
        When the number of `num_workers` processes were launched but the number of
        `num_actual workers` processes are currently running because the processing
        of some parallel worker processes was completed,  we must extrapolate the
        "Actual Rows" that is accumulated by merge_rows().
        This method extrapolates them if necessary.

        Parameters
        ----------
        leader_plan : dict
        num_actual_workers : int
          The number of the sum of launched workers and a leader process.
        num_workers : int
          The number of the sum of running workers and a leader process.
        """

        def change_rows(plan, num_actual_workers, num_workers):
            if "MergeFlag" in plan:
                if plan["MergeFlag"] == "True":
                    plan["Actual Rows"] *= float(num_workers) / float(
                        num_actual_workers
                    )

        def op(Plans, num_actual_workers, num_workers):
            if isinstance(Plans, list):
                for plan in Plans:
                    change_rows(plan, num_actual_workers, num_workers)
                    if "Plans" in plan:
                        op(plan["Plans"], num_actual_workers, num_workers)
                    return
            else:
                change_rows(Plans, num_actual_workers, num_workers)
                if "Plan" in Plans:
                    op(Plans["Plans"], num_actual_workers, num_workers)
                if "Plans" in Plans:
                    op(Plans["Plans"], num_actual_workers, num_workers)
                return

        # Main procedure.
        op(leader_plan["Plan"], num_actual_workers, num_workers)


class AddRows(Repository, PrepareMergeRows):
    def __init__(self, log_level=Log.info):
        self.ServerId = ""
        self.LogLevel = log_level

    def __set_serverId(self, serverId):
        self.ServerId = serverId

    def __add_rows(self, Plans):
        def add_rows_and_loops(plan):
            if "Workers" in plan:
                _lr = plan["Actual Rows"]
                _ll = plan["Actual Loops"]
                _wr = 0
                _wl = 0
                if plan["MergeFlag"] == "True":
                    _npr = plan["Plan Rows"] * plan["NormalizePlanParam"]
                    plan.update({"Plan Rows": _npr})
                    for _wdata in plan["Workers"]:
                        _wr += _wdata["Actual Rows"]
                        _wl += _wdata["Actual Loops"]
                plan.update({"Actual Rows": _lr + _wr})
                plan.update({"Actual Loops": _ll + _wl})

        if isinstance(Plans, list):
            for _plan in Plans:
                add_rows_and_loops(_plan)
                if "Plans" in _plan:
                    self.__add_rows(_plan["Plans"])
            return
        else:
            add_rows_and_loops(Plans)
            if "Plan" in Plans:
                self.__add_rows(Plans["Plan"])
            if "Plans" in Plans:
                self.__add_rows(Plans["Plans"])
            return

    """
    Public method
    """

    def add_rows(self, seqid, queryid, planid):
        """
        Add the parallel worker's values("Plan Rows", "Actual Rows",
        and "Actual Loops") to the leader's corresponding values in
        the plan whose seqid is `seqid`.
        """
        _jdirpath = self.get_plan_json_dir_path(self.ServerId, queryid, planid)
        _jpath = self.path(_jdirpath, str(seqid))

        if os.path.isfile(_jpath):
            if Log.notice <= self.LogLevel:
                print("Notice: seqid({}) already exists.".format(str(seqid)))
            return

        # Read plan.
        _jpath = self.path(_jdirpath, str(seqid) + ".tmp")
        if os.path.isfile(_jpath) == False:
            if Log.warning <= self.LogLevel:
                print("Warning: '{}' not found.".format(_jpath))
            return
        else:
            _dict0 = self.read_plan_json(_jpath)

        _plan = _dict0.copy()

        """
        Add three objects in each node of `_plan` to prepare to execute
        AddRows.__add_rows(), and Return numbers of workers of both
        planned and launched.
        """
        (_numPlanWorkers, _numWorkers) = self.prepare_merge_rows(_plan)

        """
        Add the values of "Plan Rows", "Actual Rows" and "Actual Loops"
        of all parallel worker's plan to the leader's corresponding values,
        respectively.
        """
        if _numWorkers > 1:
            self.__add_rows(_plan)

        # Delete 'MergeFlag' objects from _plan.
        ####self.delete_unnecessary_objects(self.delete_flags, _plan)

        # Write _plan.
        _jpath = self.path(_jdirpath, str(seqid))
        self.write_plan_json(_plan, _jpath)
        # Delete tmp file.
        if os.path.isfile(_jpath):
            _jpath = self.path(_jdirpath, str(seqid) + ".tmp")
            os.remove(_jpath)


class MergePlan(AddRows, MergeRows):
    def __init__(self, log_level=Log.info):
        self.ServerId = ""
        self.LogLevel = log_level

    def __set_serverId(self, serverId):
        self.ServerId = serverId

    """
    Public methods
    """

    def merge_plans(self, leader_plan, worker_plans):
        """
        Merge the parallel worker's plans into the leader's plan.
        This is called by QueryProgress.query_progress().

        Parameters
        ----------
        leader_plan : dict
        worker_plans : [dict, ...]

        Returns
        -------
        _leader_plan : dict
          Leader's plan whose "Plan rows" and "Actual Rows" are added
          with parallel workers' rows.
        """

        _leader_plan = leader_plan.copy()

        (_numPlanWorkers, _numWorkers) = self.prepare_merge_rows(_leader_plan)
        if len(worker_plans) > 0:
            """
            Merge the 'Plan Rows' and 'Actual Rows' of all parallel worker's
            plans(worker_plans) into the leader's plan(_leader_plan) if necessary.
            """
            self.merge_rows(_leader_plan, worker_plans)

        """
        If some parallel workers complete the processing, we extrapolate the
        "Actual Rows" of `_leader_plan` because the values of the terminated
        processes are not added by merge_rows().
        """
        if len(worker_plans) + 1 < _numWorkers:
            self.extrapolate_rows(_leader_plan, len(worker_plans) + 1, _numWorkers)

        return _leader_plan

    def add_workers_rows(self, serverId, current_seqid, max_seqid):
        """
        For the plans with seqid `current_seqid` to `max_seqid`, add the parallel
        worker's values("Plan Rows", "Actual Rows", and "Actual Loops") to the
        leader's corresponding values in each plan.

        This is called by GetTables.get_tables().

        Parameters
        ----------
        serverId : str
          The serverId of the database server that is described in the hosts.conf.
        current_seqid : int
          The maximum seqid for plans already stored in the repository.
        max_seqid : int
          The maximum seqid for plans stored in the database.

        Returns
        -------
        None
        """

        self.__set_serverId(serverId)

        """
        Read log.csv to get the json plans whose seqid are between current_seqid
        and max_seqid.
        """
        with open(self.get_log_csv_path(self.ServerId), newline="") as f:
            _reader = csv.reader(f, delimiter=",", quoting=csv.QUOTE_NONE)
            for row in _reader:
                _seqid = int(row[0])
                _queryid = int(row[6])
                _planid = int(row[7])
                if current_seqid < _seqid and _seqid <= max_seqid:
                    self.add_rows(_seqid, _queryid, _planid)
        f.close()
