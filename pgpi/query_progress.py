"""
query_progress.py


  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 2021, Hironobu Suzuki @ interdb.jp
"""

import json
import math
import operator

from .common import Common, State, Log
from .repository import Repository
from .replace import Replace
from .regression import Regression
from .merge_plan import MergePlan
from .rules import Rules


class Node(Common):
    """
    Provide methods for calculating 'Plan Points' and 'Actual Points'.
    """

    def __init__(self, regression=False, log_level=Log.error):
        self.LogLevel = log_level
        self.regression = regression

    def __O_N(self, n):
        return n

    def __O_1(self, n):
        return 1

    def __O_NlogN(self, n):
        return n * math.log(int(n))

    def __simpleOp(self, target, plan, f):
        _removedRows = self.count_removed_rows(target)
        if self.regression:
            target["PlanPoints"] = f(max(target["Plan Rows"], target["Actual Rows"]))
            target["ActualPoints"] = f(target["Actual Rows"])
        else:
            """
            This part is executed if no using regression parameters, so we should
            estimate the accurate 'Plan Rows' on the fly even during the query
            processing.

            'ExpectedRows' stores the estimated 'Plan Rows'.
            """
            if target["CurrentState"] == State.FINISHED:
                target["ExpectedRows"] = plan["Actual Rows"]
                target["PlanPoints"] = f(target["Actual Rows"] + _removedRows)
                target["ActualPoints"] = f(target["PlanPoints"])
            else:
                target["ExpectedRows"] = max(plan["Plan Rows"], plan["Actual Rows"])
                target["PlanPoints"] = f(target["ExpectedRows"] + _removedRows)
                target["ActualPoints"] = f(target["Actual Rows"] + _removedRows)

    def __simpleListOp(self, plan, f):
        if "Plans" not in plan:
            self.__simpleOp(plan, plan, f)
        else:
            _plans = plan["Plans"]
            if isinstance(_plans, list):
                for p in _plans:
                    self.__simpleOp(plan, p, f)
            else:
                self.__simpleOp(plan, _plans, f)

    """
    Join operations
    """

    def __outer(self, outer, inner):
        return outer

    def __heuristics(self, plan_rows, estimate_rows, actual_rows):
        return max(plan_rows, actual_rows)

    def __joinOp_with_estimating_plan_rows_on_the_fly(
        self, plan, estimate_rows, estimatePoints, removed_rows
    ):
        """
        This method is called if no using regression parameters, so we should
        estimate the accurate 'Plan Rows' on the fly even during the query
        processing.

        'ExpectedRows' stores the estimated 'Plan Rows' and the estimating method
        is heuristics.
        """
        _plans = plan["Plans"]
        if isinstance(_plans, list):
            _expected_rows = []
            _plan_points = []
            _actual_points = []
            for p in _plans:
                _expected_rows.append(p["ExpectedRows"])
                _plan_points.append(p["PlanPoints"])
                _actual_points.append(p["ActualPoints"])

            _estimated_points = estimatePoints(_expected_rows[0], _expected_rows[1])
            plan["PlanPoints"] = _estimated_points
            if plan["CurrentState"] == State.FINISHED:
                plan["ExpectedRows"] = plan["PlanPoints"]
                plan["ActualPoints"] = plan["Actual Rows"]
            else:
                if (_estimated_points - removed_rows) == plan["Actual Rows"]:
                    plan["ExpectedRows"] = plan["Actual Rows"]
                    plan["ActualPoints"] = _estimated_points
                else:
                    plan["ExpectedRows"] = estimate_rows(
                        plan["Plan Rows"],
                        (_estimated_points - removed_rows),
                        plan["Actual Rows"],
                    )
                    plan["ActualPoints"] = plan["Actual Rows"] + removed_rows
        else:
            print("******Never reach********")

    def __joinOp_with_estimated_plan_rows(self, plan, removed_rows):
        """
        This method is called when the "Plan Rows" in all nodes are already replaced
        by the estimated Plan Rows using the regression parameters.
        Therefore, 'ExpectedRows' is not needed.
        """

        def get_children(plan):
            _PR = [[], []]
            _AR = [[], []]
            for i in range(0, 2):  # Ignore SubPlans.
                p = plan["Plans"][i]
                k = 0 if p["Parent Relationship"] == "Outer" else 1
                _PR[k] = p["Plan Rows"]
                _AR[k] = p["Actual Rows"]
            return (_PR[0], _PR[1], _AR[0], _AR[1])

        (PlanOuter, PlanInner, ActualOuter, ActualInner) = get_children(plan)
        if PlanOuter < ActualOuter:
            PlanOuter = ActualOuter
        if PlanInner < ActualInner:
            PlanInner = ActualInner

        if "Hash Join" == plan["Node Type"] or "Merge Join" == plan["Node Type"]:
            _coef = plan["Coefficient"]
            _coef2 = plan["Coefficient2"]
            _intercept = plan["Intercept"][0]
            if _coef2 > 0:
                plan["PlanPoints"] = PlanOuter * PlanInner
                plan["ActualPoints"] = ActualOuter * ActualInner + removed_rows
            else:
                plan["PlanPoints"] = PlanOuter + PlanInner
                plan["ActualPoints"] = ActualOuter + ActualInner + removed_rows
        else:  # "Nested Loop"
            plan["PlanPoints"] = PlanOuter * PlanInner
            plan["ActualPoints"] = ActualOuter * ActualInner + removed_rows

        if Log.debug1 <= self.LogLevel:
            print(
                "Debug1: __joinOp3 type={} PlanPoints={} ActualPoints={}".format(
                    plan["Node Type"], plan["PlanPoints"], plan["ActualPoints"]
                )
            )

    def __joinOp(self, plan, estimate_rows, estimatePoints):
        removed_rows = self.count_removed_rows(plan)
        if self.regression:
            self.__joinOp_with_estimated_plan_rows(plan, removed_rows)
        else:
            self.__joinOp_with_estimating_plan_rows_on_the_fly(
                plan, estimate_rows, estimatePoints, removed_rows
            )

    """
    Public method
    """

    def calc(self, plan, regression):
        self.regression = regression
        _nodeType = plan["Node Type"]

        # For the nodes that have one or no child (plan).
        if _nodeType == "Result":
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "Seq Scan":
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "Sample Scan":
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "Index Scan":
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "Index Only Scan":
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "Bitmap Index Scan":
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "Bitmap Heap Scan":
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "Tid Scan":
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "Function Scan":
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "Table Function Scan":
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "Values Scan":
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "CTE Scan":
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "Named Tuplestore Scan":
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "WorkTable Scan":  # TODO: Need to check
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "Foreign Scan":  # TODO: Need to check
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "Aggregate":
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "SetOp":
            self.__simpleOp(plan, plan, self.__O_N)
        elif _nodeType == "Limit":
            self.__simpleOp(plan, plan, self.__O_N)

        # For the nodes that have one child (plan).
        elif _nodeType == "Hash":
            self.__simpleListOp(plan, self.__O_N)
        elif _nodeType == "ProjectSet":
            self.__simpleListOp(plan, self.__O_N)
        elif _nodeType == "Subquery Scan":
            self.__simpleListOp(plan, self.__O_N)
        elif _nodeType == "Custom Scan":  # TODO: Need to check
            self.__simpleListOp(plan, self.__O_N)
        elif _nodeType == "Materialize":
            self.__simpleListOp(plan, self.__O_N)
        elif _nodeType == "Sort":
            self.__simpleListOp(plan, self.__O_NlogN)
        elif _nodeType == "Incremental Sort":
            self.__simpleListOp(plan, self.__O_NlogN)
        elif _nodeType == "Gather":
            self.__simpleListOp(plan, self.__O_N)
        elif _nodeType == "Gather Merge":
            self.__simpleListOp(plan, self.__O_N)
        elif _nodeType == "LockRows":
            self.__simpleListOp(plan, self.__O_N)
        elif _nodeType == "Unique":
            self.__simpleListOp(plan, self.__O_N)
        elif _nodeType == "WindowAgg":
            self.__simpleListOp(plan, self.__O_N)

        # For the nodes that have two children (plans).
        elif _nodeType == "Append":
            self.__joinOp(plan, max, operator.add)
        elif _nodeType == "Merge Append":
            self.__joinOp(plan, max, operator.add)
        elif _nodeType == "Recursive Union":
            self.__joinOp(plan, max, operator.mul)
        elif _nodeType == "Nested Loop":
            self.__joinOp(plan, max, operator.mul)
        elif _nodeType == "Merge Join":
            self.__joinOp(plan, self.__heuristics, self.__outer)
        elif _nodeType == "Hash Join":
            self.__joinOp(plan, self.__heuristics, operator.add)
        elif _nodeType == "BitmapAnd":
            self.__joinOp(plan, max, operator.add)
        elif _nodeType == "BitmapOr":
            self.__joinOp(plan, max, operator.add)


class CalcNode(Node):
    """
    Provide methods to estimate the progress of the query.

    Estimation Outline:

    To estimate the progress of the query, this module defines a concept internally
    called 'point'. It is a non-dimensional value, and it is like the concept of
    cost in the optimizer in the field of database systems.

    The point is simpler than the cost, it increments 1 when a row is read by scan
    type operations and when a row is processed by join and scan type operations.
    Examples:
      1. If 100 rows are read by Seq Scan, the point increments 100.
      2. If Nested Loop Join processes the outer 100 rows and the inner 20 rows,
    the point increments 2000.

    When estimating the progress of the query, the 'Plan Points' and the
    'Actual Points' are calculated in each node by the calc_node() method, where
    the 'Plan Points' is the estimated point to get eventually, and the
    'Actual Points' is the actual point.

    Then, all 'Plan Points' and 'Actual Points' are counted up in all nodes by the
    count_point() method, and returns 'Actual Points' / 'Plan Points' as the
    progress of the query.
    """

    def __init__(self, log_level=Log.error):
        self.LogLevel = log_level
        self.regression = False

    def __set_objects(self, plan, state, is_outer_running):
        """
        Add four objects in each node: "ExpectedRows", "ActualPoints", "PlanPoints"
        and "CurrentState"; set the appropriate state to the 'CurrentState' object.
        """

        """Delete unnecessary objects."""
        for _i in (
            "Startup Cost",
            "Total Cost",
            "Plan Width",
            "Actual Startup Time",
            "Actual duration Time",
            "BufferUsage_Start",
            "WalUsage_Start",
            "BufferUsage",
            "WalUsage",
        ):
            if _i in plan:
                del plan[_i]

        """Add 'ExpectedRows', 'ActualPoints' and 'PlanPoints'."""
        if "Actual Rows" in plan:
            plan.update(ExpectedRows=0, ActualPoints=0, PlanPoints=0)

            if Log.debug5 <= self.LogLevel:
                print("Debug5: NodeType={}".format(plan["Node Type"]))

            """
            Add an object 'CurrentState' and Set the appropriate state.

            If no use of the regression parameters to estimate the progress,
            the CurrentState can be changed by applying the rules after this step,
            therefore, the setting state is tentative.
            Otherwise, i.e. if using the regression parameters, the value
            of CurrentState is not used.
            """
            if state == State.WAITING:
                if (
                    plan["Actual Rows"] == 0
                    and plan["Actual Loops"] == 0
                    and self.count_removed_rows(plan) == 0
                ):
                    if Log.debug5 <= self.LogLevel:
                        print(
                            "Debug5:   WAITING -> WAITING ActualRows == 0 "
                            + "AND ActualLoops == 0 AND RemovedRows == 0"
                        )
                    plan.update(CurrentState=state)
                    return state
                else:
                    if Log.debug5 <= self.LogLevel:
                        print(
                            "Debug5:   WAITING -> RUNNING ActualRows != 0 "
                            + "OR ActualLoops != 0 OR RemovedRows != 0"
                        )
                    plan.update(CurrentState=State.RUNNING)
                    return State.RUNNING
            elif state == State.RUNNING:
                if ((is_outer_running)) and self.isScan(plan):
                    if Log.debug5 <= self.LogLevel:
                        print("Debug5:   RUNNING -> FINISHED  Outer running AND scan")
                    plan.update(CurrentState=State.FINISHED)
                    return State.FINISHED
                else:
                    if Log.debug5 <= self.LogLevel:
                        print("Debug5:   RUNING -> RUNING  !Outer running OR !scan")
                    plan.update(CurrentState=State.RUNNING)
                    return state
            else:  # state == State.FINISHED
                if Log.debug5 <= self.LogLevel:
                    print("Debug5:   FINISHED -> FINISHED")
                plan.update(CurrentState=State.FINISHED)
                return state
        else:
            return state

    """
    Public method
    """

    def prepare_calc_node(self, plans, regression=False):
        """
        Prepare to execute the calc_node() method. That is, add four objects,
        which are described in the __set_objects() method, in all nodes of plans,
        and set the appropriate state to the 'CurrentState' object.
        """

        def op(Plans, state):
            if isinstance(Plans, list):
                _is_outer_running = False
                for plan in Plans:
                    _state = self.__set_objects(plan, state, _is_outer_running)
                    if Log.debug3 <= self.LogLevel:
                        print(
                            "Debug3: NodeType={}   CurrentState={}".format(
                                plan["Node Type"], plan["CurrentState"]
                            )
                        )
                    _is_outer_running = True if plan["Actual Loops"] > 0 else False
                    if "Plans" in plan:
                        op(plan["Plans"], _state)
                return
            else:
                _state = self.__set_objects(Plans, state, True)
                if Log.debug3 <= self.LogLevel:
                    print(
                        "Debug3: NodeType={}   CurrentState={}".format(
                            Plans["Node Type"], Plans["CurrentState"]
                        )
                    )
                if "Plans" in Plans:
                    op(Plans["Plans"], _state)
                return

        if regression:
            op(plans, State.FINISHED)
        else:
            op(plans, State.WAITING)
        return plans

    def calc_node(self, plans, depth, regression):
        """
        Dig down the plan tree to the specific node, which is specified by depth,
        and Calculate the 'Plan Points' and the 'Actual Points'.
        """

        def incr(plan):
            if "Node Type" in plan:
                self._count += 1

        def op(Plans):
            if isinstance(Plans, list):
                for plan in Plans:
                    incr(plan)
                    if self._depth == self._count:
                        self.calc(plan, self.regression)
                        return
                    elif "Plans" in plan:
                        op(plan["Plans"])
            else:
                incr(Plans)
                if self._depth == self._count:
                    self.calc(Plans, self.regression)
                    return
                elif "Plans" in Plans:
                    op(Plans["Plans"])

        # Main procedure.
        self._depth = depth
        self._count = 0
        self.regression = regression
        op(plans)

    def count_points(self, plans):
        """
        Count up the 'Plan Points' and the 'Actual Points', and Return the proportion
        of the actual and planned points.
        """

        def op(Plans):
            """Count up the planned and actual points recursively."""
            self._i += 1
            if isinstance(Plans, list):
                for plan in Plans:
                    if "Plans" in plan:
                        if "ActualPoints" in plan:
                            self._plan_points += plan["PlanPoints"]
                            self._actual_points += plan["ActualPoints"]
                        op(plan["Plans"])
            else:
                if "Plans" in Plans:
                    if "ActualPoints" in Plans:
                        self._plan_points += Plans["PlanPoints"]
                        self._actual_points += Plans["ActualPoints"]
                    op(Plans["Plans"])
            return

        # Main procedure.
        self._i = 0
        self._actual_points = 0
        self._plan_points = 0
        op(plans)
        if self._plan_points == 0:
            return 0.0
        return min(self._actual_points / self._plan_points, 1)


"""
QueryProgress
"""


class QueryProgress(MergePlan, Replace, Rules, CalcNode):
    def __init__(self, base_dir=".", log_level=Log.error):
        self.set_base_dir(base_dir)
        self.LogLevel = log_level

    def _progress(self, merged_plan, serverId=None, queryid=None, planid=None):
        """
        Estimate the progress of merged_plan.
        This is called by query_progress() and check()@tools/sampling_plan.py.

        Parameters
        ----------
         merged_plan : dict

        Returns
        -------
        progress : float
          0.0 <= progress <= 1.0
        """

        def delete_objects(plan):
            for _i in (
                "Parallel Aware",
                "Relation Name",
                "Alias",
                "Startup Cost",
                "Total Cost",
                "Plan Width",
                "Scan Direction",
                "Index Name",
                "Index Cond",
                "Heap Fetches",
                "Original Hash Buckets",
                "Hash Batches",
                "Original Hash Batches",
                '"Hash Buckets',
                "Hash Cond",
                "Join Filter",
                "Peak Memory Usage",
                "Inner Unique",
            ):
                if _i in plan:
                    del plan[_i]
            return plan

        # Main procedure.
        Plans = merged_plan["Plan"]
        self.delete_unnecessary_objects(delete_objects, Plans)

        _numNode = self.count_nodes(Plans)
        _regression = False

        if serverId is not None and queryid is not None and planid is not None:
            _reg_param = self.get_regression_param(serverId, queryid, planid)
        else:
            _reg_param = None

        if _reg_param is not None:

            if self.check_formatted_regression_params(serverId, queryid):
                if Log.info <= self.LogLevel:
                    print("Info: Using formatted regression params.")

            else:
                _regression = True
                """
                If there are already the regression parameters of this query,
                replace the Plan Rows with the estimated rows using the regression parameters.
                """
                self.replace_plan_rows(
                    merged_plan["Plan"], _reg_param["Plan"], _numNode, queryid, planid
                )
                if Log.info <= self.LogLevel:
                    print("Info: Using regression params.")

        else:
            if Log.info <= self.LogLevel:
                print("Info: Using rules.")

        """
        Cut the top node if the node type is 'ModifyTable', i.e. the query is INSERT, DELETE or UPDATE.
        """
        if Plans["Node Type"] == "ModifyTable":
            _numNode -= 1
            if "Plans" in Plans:
                _plans = Plans["Plans"]
                if isinstance(_plans, list):
                    for _plan in _plans:
                        fp = _plan  # Choice the first one.
                        break
                else:
                    fp = _plans
                Plans = fp

        """
        Prepare the nodes of Plans to calculate the "Plan Points"
        and "Actual Points".
        """
        Plans = self.prepare_calc_node(Plans, _regression)

        if not _regression:
            # Apply custom rules.
            self.apply_rules(Plans)

        """
        Calculate the "Plan Points" and "Actual Points" in order,
        from the bottom node to the top node.
        """
        _i = _numNode
        while 0 < _i:
            self.calc_node(Plans, _i, _regression)
            _i -= 1

        """
        Count up the "Plan Points" and "Actual Points".
        """
        return round(self.count_points(Plans), 6)

    """
    Public methods
    """

    def make_progress_bar(self, percent, small=False):
        """
        Make the value of percent with progress-bar.

        Parameters
        ----------
        percent : int or float
          0.0 <= percent <= 100.0

        small : bool
          If true, the range of progress-bar is 25 [byte]; otherwise 50 [byte].

        Returns
        -------
        _p_bar : str
          progress-bar.
        """
        qSteps = [
            " ",
            u"\u258e",
            u"\u258d",
            u"\u258b",
        ]

        percent = max(0, percent)
        percent = min(100, percent)
        _p = int(percent)

        _p_bar = str(u"\u258e") if _p == 0 else ""
        for _i in range(
            0, (lambda p, small: (p // 4) if small else (p // 2))(_p, small)
        ):
            _p_bar += str(u"\u2588")
        _p_bar += str(
            qSteps[(lambda p, small: (p % 4) if small else (p % 2) * 2)(_p, small)]
        )
        for _i in range(
            (
                lambda p, small: 2
                if p == 0
                else ((p // 4 + 1) if small else (_p // 2 + 1))
            )(_p, small),
            (lambda small: 25 if small else 50)(small),
        ):
            _p_bar += " "
        if _p < 100:
            _p_bar += str(u"\u258f")
        return _p_bar

    def query_progress(self, plan_list, server_id=None):
        """
        Estimate the progresses of the plans in plan_list.

        Note: Don't use for EXPLAIN ANALYZE statement because this method will crash.

        Parameters
        ----------
        plan_list : [results, ... ]
          A list of the results of pg_query_plan().
            results := [
                          worker_type ->str,
                          queryid ->int,
                          planid ->int,
                          plan_json ->str,
                          hash_query ->int
                        ]

        Returns
        -------
        progress : [float, ...]
          A list of progresses.
        """

        def set_queryid_to_parallel_worker(plan_list):
            """
            In PG version 13, the queryIds of parallel workers are always set to 0,
            so, we should set the leader's queryId to the parallel workers' queryIds.
            Note:
            If nested_level = 0 for all queries, it is easy to set the parallel
            worker's queryid. Otherwise, i.e. there are queries whose nested_level
            are more than 1, it is not easy to associate the queryid of the leader
            with the queryids of parallel workers.
            This function uses the hash values of the query strings in order to
            associate leader's query with parallel worker's queries.
            """
            _hash = {}
            for _plan in plan_list:
                _queryid = _plan[1]
                _hash_query = _plan[4]
                if _queryid != 0:
                    _hash[str(_hash_query)] = _queryid
            for _plan in plan_list:
                _queryid = _plan[1]
                _hash_query = _plan[4]
                if _queryid == 0:
                    _plan[1] = _hash[str(_hash_query)]

        def get_unique_queryid(plan_list):
            """
            Get the unique queryIds in the plan_list.
            Examples:
              1. If plan_list has three plans of which are a leader and
                 two parallel workers, this method returns only the queryId
                 of the leader because all queryId are the same value.
              2. If plan_list has two plans which are different nested levels,
                 This method returns two queryIds because the two plans are
                 different queries in each.
            """
            _tmp = []
            for _plan in plan_list:
                _queryid = _plan[1]
                _tmp.append(int(_queryid))
            _ret = sorted(set(_tmp))
            return _ret

        def prepare_merge_plans(queryid, plan_list):
            """
            Separate the leader's plan and the worker's plan from plan_list,
            and transfer from str type to dict type.
            Then, return them with their planId.
            """
            _worker_plans = []
            for _plan in plan_list:
                _worker_type = _plan[0]
                _queryid = _plan[1]
                _plan_json = _plan[3]
                if _queryid == queryid:
                    if _worker_type == "leader":
                        _planid = _plan[2]
                        _leader_plan = json.loads(_plan_json)
                    else:
                        _worker_plans.append(json.loads(_plan_json))
            """If there is no parallel worker, _worker_plans is an empty list."""
            return (_planid, _leader_plan, _worker_plans)

        # Main procedure.
        set_queryid_to_parallel_worker(plan_list)  # For version 13.
        _ret = []
        for _queryid in get_unique_queryid(plan_list):
            (_planid, _leader_plan, _worker_plans) = prepare_merge_plans(
                _queryid, plan_list
            )
            _merged_plan = self.merge_plans(_leader_plan, _worker_plans)
            _progress = self._progress(_merged_plan, server_id, _queryid, _planid)
            if Log.debug1 <= self.LogLevel:
                print("Debug1: queryid={}  => progress={}".format(_queryid, _progress))
            _ret.append((_queryid, _progress))
        return _ret
