"""
rgression.py


  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 2021-2023, Hironobu Suzuki @ interdb.jp
"""

import math
import os
import operator

from .common import Common, Log
from .repository import Repository
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error


class CalcRegression:
    """
    Functions to calculate the regression params.
    """

    def set_log_level(self, log_level):
        self.LogLevel = log_level

    def __rss(self, X, Y):
        assert len(X) != 0
        return math.sqrt(
            sum(list(map(lambda x: x ** 2, list(map(operator.sub, X, Y)))))
        ) / len(X)

    def scan(self, X, Y):
        """
        linear regression
          * Model(no bias): y = a * x
          * Loss function: Mean Square Error
        """
        _sumY = sum(Y)
        _sumX = sum(X)
        if Log.debug3 <= self.LogLevel:
            print("Debug3: ----- SCAN ----")
            print("Debug3:       ===> X = {}".format(X))
            print("Debug3:       ===> Y = {}".format(Y))
        if 250 * _sumY < _sumX:
            # Assume that this data set is fit to a constant function.
            if Log.debug3 <= self.LogLevel:
                print(
                    "Debug3:       ==> coef = 0    intercept = {}".format(
                        float(round(_sumY / len(Y), 5))
                    )
                )
            return (0.0, float(round(_sumY / len(Y), 5)))
        else:
            if Log.debug3 <= self.LogLevel:
                if _sumX == 0:
                    print(
                        "Debug3:       ==> coef = 0   intercept = {}".format(
                            float(round(_sumY / len(Y), 5))
                        )
                    )
                else:
                    print(
                        "Debug3:       ==> coef = {}  intercept = 0".format(
                            float(round(_sumY / _sumX, 5))
                        )
                    )

            if _sumX == 0:
                return (0.0, float(round(_sumY / len(Y), 5)))
            else:
                return (float(_sumY / _sumX), 0.0)

    def gather(self, X, Y):
        """
        linear regression
          * Model(no bias): y = a * x
          * Loss function: Mean Square Error
        """
        _sumY = sum(Y)
        _sumX = sum(X)

        if Log.debug3 <= self.LogLevel:
            print("Debug3: ---- GATHER ----")
            print("Debug3:       ===> X = {}".format(X))
            print("Debug3:       ===> Y = {}".format(Y))
        if Log.debug3 <= self.LogLevel:
            if _sumX == 0:
                print(
                    "Debug3:       ==> coef = 0   intercept = {}".format(
                        float(round(_sumY / len(Y), 5))
                    )
                )
            else:
                print(
                    "Debug3:       ==> coef = {}  intercept = 0".format(
                        float(round(_sumY / _sumX, 5))
                    )
                )

        if _sumX == 0:
            return (0.0, float(round(_sumY / len(Y), 5)))
        else:
            return (float(_sumY / _sumX), 0.0)

    def nested_loop(self, Xouter, Xinner, Y):
        """
        Multiple linear regression
          * Model(no bias): Y = a * Xinner * Xouter
          * Loss function: Mean Square Error
        """
        """
        _sumY = 0; _sumX = 0
        for i in range(0, len(Y)):
            _sumY += Y[i] * Xinner[i] * Xouter[i]
            _sumX += Xinner[i] **2 * Xouter[i] **2
        """
        _sumY = sum(list(map(operator.mul, list(map(operator.mul, Xinner, Xouter)), Y)))
        _sumX = sum(
            list(
                map(
                    operator.mul,
                    list(map(lambda x: x ** 2, Xinner)),
                    list(map(lambda x: x ** 2, Xouter)),
                )
            )
        )
        if Log.debug3 <= self.LogLevel:
            print("Debug3: +++++ NESTED LOOP JOIN +++++")
            print("Debug3:       ===> Xouter = {}".format(Xouter))
            print("Debug3:       ===> Xinner = {}".format(Xinner))
            print("Debug3:       ===>      Y = {}".format(Y))
            if _sumX == 0:
                print("Debug3:       ==> coef=1")
            else:
                print("Debug3:       ==> coef={}".format(str(round(_sumY / _sumX, 5))))

        return 1.0 if _sumX == 0 else float(_sumY / _sumX)

    def merge_or_hash_join(self, Xouter, Xinner, Y, add_bias_0=True):
        def multi_regression(Xouter, Xinner, Y, add_bias_0=True):
            _X = []
            _Y = []

            """Format _Y and _X"""
            for i in range(0, len(Y)):
                _Y.append(Y[i])
                _X.append([Xouter[i], Xinner[i]])
            if add_bias_0:
                # Add a constraint because we assume that the bias is 0
                _X.append([0.0, 0.0])
                _Y.append(0.0)
            if Log.debug3 <= self.LogLevel:
                print("Debug3: ****MERGE OR HASH JOIN*****")
                print("Debug3:       ===> Xouter = {}".format(Xouter))
                print("Debug3:       ===> Xinner = {}".format(Xinner))
                print("Debug3:       ===> X ={}".format(_X))
                print("Debug3:       ===> Plan Rows ={}  Y={}".format(Y, _Y))

            """
            Calc regression
            Multiple linear regression
            * Model(no bias): Y = a1 * Xouter + a2 * Xinner
            * Loss function: Mean Square Error
            """
            scireg = LinearRegression()
            scireg.fit(_X, _Y)
            _list = scireg.coef_.tolist()
            _coef = [float(round(_list[n], 5)) for n in range(len(_list))]
            _intercept = float(round(scireg.intercept_ + 0.0, 5))

            """Predict and calculate RMSE."""
            _y_pred = scireg.predict(_X)
            _rmse = np.sqrt(mean_squared_error(_Y, _y_pred))

            del scireg
            return (_coef, _intercept, _rmse)

        def single_regression(X, Y, add_bias_0=True):
            _X = []
            _Y = []

            """Format _Y and _X"""
            for i in range(0, len(Y)):
                _Y.append(Y[i])
                _X.append([X[i]])
            if add_bias_0:
                # Add a constraint because we assume that the bias is 0
                _X.append([0.0])
                _Y.append(0.0)
            if Log.debug3 <= self.LogLevel:
                print("Debug3: ****MERGE OR HASHOIN*****")
                print("Debug3:       ===> X={}".format(X))
                print("Debug3:       ===> Plan Rows ={}  Y={}".format(Y, _Y))

            """
            Calc regression
            Multiple linear regression
            * Model: Y = a * X + b
            * Loss function: Mean Square Error
            """
            scireg = LinearRegression()
            scireg.fit(_X, _Y)
            _list = scireg.coef_.tolist()
            _coef = [float(round(_list[n], 5)) for n in range(len(_list))]
            _intercept = float(round(scireg.intercept_ + 0.0, 5))

            """Predict and calculate RMSE."""
            _y_pred = scireg.predict(_X)
            _rmse = np.sqrt(mean_squared_error(_Y, _y_pred))

            del scireg
            return (float(_coef[0]), float(_intercept), _rmse)

        def reg(Xouter, Xinner, Y):
            ## Same as NestedLoop
            _sumY = sum(
                list(map(operator.mul, list(map(operator.mul, Xinner, Xouter)), Y))
            )
            _sumX = sum(
                list(
                    map(
                        operator.mul,
                        list(map(lambda x: x ** 2, Xinner)),
                        list(map(lambda x: x ** 2, Xouter)),
                    )
                )
            )
            _coef = 1.0 if _sumX == 0 else float(_sumY / _sumX)
            # Calculate MSE
            _mse = (
                sum(
                    list(
                        map(
                            lambda x: x ** 2,
                            list(
                                map(
                                    operator.sub,
                                    Y,
                                    list(
                                        map(
                                            lambda y: _coef * y,
                                            list(map(operator.mul, Xouter, Xinner)),
                                        )
                                    ),
                                )
                            ),
                        )
                    )
                )
                / len(Y)
            )

            return (_coef, np.sqrt(_mse))

        """
        Calcuate regression parameters.
        """
        (coef, intercept, rmse) = multi_regression(Xouter, Xinner, Y)
        if coef[0] < 0 or coef[1] < 0:
            (coef, intercept, rmse) = multi_regression(Xouter, Xinner, Y, False)
        _coef = [float(coef[0]), float(coef[1])]
        _reg = 0
        _intercept = float(round(intercept + 0.0, 5))
        _rmse = rmse

        (coef, intercept, rmse) = single_regression(Xouter, Y)
        if coef < 0:
            (coef, intercept, rmse) = single_regression(Xouter, Y, False)
        if rmse < _rmse:
            _coef = [float(coef), 0.0]
            _intercept = float(round(intercept + 0.0, 5))
            _rmse = rmse

        (coef, intercept, rmse) = single_regression(Xinner, Y)
        if coef < 0:
            (coef, intercept, rmse) = single_regression(Xinner, Y, False)
        if rmse < _rmse:
            _coef = [0.0, float(coef)]
            _intercept = float(round(intercept + 0.0, 5))
            _rmse = rmse

        """
        # Note: This is not used because it makes the results significantly unstable.
        (coef, rmse) = reg(Xouter, Xinner, Y)
        if rmse < _rmse:
            _coef = [0, 0]
            _intercept = 0
            _reg = coef
        """

        if Log.debug3 <= self.LogLevel:
            print(
                "Debug3:       ==> coef={} reg={}   intercept={}".format(
                    _coef, _reg, _intercept
                )
            )

        return (_coef, _reg, _intercept)


class Regression(Repository, CalcRegression):
    def __init__(self, base_dir=".", log_level=Log.error):
        self.ServerId = ""
        self.Level = 0
        self.set_base_dir(base_dir)
        self.LogLevel = log_level

    def __set_serverId(self, serverId):
        self.ServerId = serverId

    """
    Handle self.Level value.
    """

    def __init_level(self):
        self.Level = 0

    def __incr_level(self):
        self.Level += 1

    def __get_level(self):
        return self.Level

    def __delete_objects(self, plan):
        """Delete all objects except 'Node Type', 'Plan(s)' and some."""

        for k in list(plan):
            """
            Use list(plan) instead of plan.keys() to avoid
            "RuntimeError: dictionary changed size during iteration" error.
            """
            if (
                k != "Node Type"
                and k != "Plans"
                and k != "Plan"
                and k != "Relation Name"
                and k != "Schema"
                and k != "Alias"
                and k != "Parent Relationship"
                and k != "MergeFlag"
            ):
                plan.pop(k)
        return plan

    def __calc_regression(self, plan, reg, queryid, planid, depth):
        """
        Calculate the regression parameters of plan, and Set the results into reg.
        """

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
                """
                Calculate the regression parameter.
                """
                if Log.debug3 <= self.LogLevel:
                    print("Debug3: === NodeType={}".format(n))
                    print("Debug3: *** Y ActualRows={}".format(plan["Actual Rows"]))
                    print(
                        "Debug3: *** Xouter ={}    Xinner ={}".format(_Xouter, _Xinner)
                    )

                _Y = plan["Actual Rows"]
                _coef = self.nested_loop(_Xouter, _Xinner, _Y)

                """
                Set the result to the reg dict.
                """
                if type(_coef) is list:
                    reg.update(Coefficient=_coef)
                else:
                    reg.update(Coefficient=[_coef])
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
                """
                Calculate the  regression parameter.
                """
                if Log.debug3 <= self.LogLevel:
                    print(
                        "Debug3: HASH or MERGE depth={}  RR={}  queryid={} planid={}".format(
                            depth, _RR, queryid, planid
                        )
                    )
                if Log.debug3 <= self.LogLevel:
                    print("Debug3: === NodeType={}".format(n))
                    print("Debug3: *** Y ActualRows={}".format(plan["Actual Rows"]))
                    print(
                        "Debug3: *** Xouter ={}    Xinner ={}".format(_Xouter, _Xinner)
                    )

                _Y = plan["Actual Rows"]
                (_coef, _reg, _intercept) = self.merge_or_hash_join(
                    _Xouter, _Xinner, _Y
                )

                """
                Set the result to the reg dict.
                """
                if type(_coef) is list:
                    reg.update(Coefficient=_coef)
                else:
                    reg.update(Coefficient=[_coef])
                reg.update(Coefficient2=[round(_reg + 0.0, 5)])
                reg.update(Intercept=[round(_intercept + 0.0, 5)])

                return

        """
        scan type
        """

        """Calculate the regression parameter."""
        if Log.debug3 <= self.LogLevel:
            print("Debug3: === NodeType={}".format(_node_type))
            print(
                "Debug3: *** Plan Rows={}      NormalizeParam={}      NormalizePlanParam={}".format(
                    plan["Plan Rows"],
                    plan["NormalizeParam"],
                    plan["NormalizePlanParam"],
                )
            )
            print("Debug3: *** Actual Rows={}".format(plan["Actual Rows"]))

        (_coef, _intercept) = self.scan(plan["Plan Rows"], plan["Actual Rows"])

        """
        Set the result to the reg dict.
        """
        if type(_coef) is list:
            reg.update(Coefficient=_coef)
        else:
            reg.update(Coefficient=[_coef])
        reg.update(Intercept=[round(_intercept + 0.0, 5)])
        return

    def __set_relations(self, Plans, depth):
        """
        Set "Relation Name" in Plans by gathering children's "Relation Name" up if Plans does not have it.

        For example, if the node type of Plans is "Sort" and the node type of Plans' child is "Seq Scan",
        the relation name of Plans is set to the relation name of Plans' child.

        "Node Type":"Sort"  ==> "Node Type":"Sort", "Relation Name":"tbl1"
          -> "Node Type":"Seq Scan", "Relation Name":"tbl1"


        If the node type of Plans is "Nested Loop", the relation name of Plans is set to the pair of the
        relation names of Plans' outer and inner children.

        "Node Type":"Nested Loop" ==> "Node Type":"Nested Loop","Relation Name":"[tbl1, tbl2]"
          -> "Node Type":"Seq Scan", "Relation Name":"tbl1"
          -> "Node Type":"Seq Scan", "Relation Name":"tbl2"

        "Node Type":"Merge Join" ==>  "Node Type":"Merge Join", "Relation Name":"[[tbl1, tbl2], tbl3]"
          -> "Node Type":"Nested Loop","Relation Name":"[tbl1, tbl2]"
               -> "Node Type":"Seq Scan", "Relation Name":"tbl1"
               -> "Node Type":"Seq Scan", "Relation Name":"tbl2"
          -> "Node Type":"Seq Scan", "Relation Name":"tbl3"
        """

        def get_relations(plan):
            if "Relation Name" not in plan:
                if "Plans" in plan:
                    __plan = plan["Plans"]
                elif "Plan" in plan:
                    __plan = plan["Plan"]
                else:
                    return
                if isinstance(__plan, list):
                    __outer_plan = __plan[0]
                    __inner_plan = __plan[1] if 2 <= len(__plan) else None
                    if __inner_plan is None:
                        if "Relation Name" in __outer_plan:
                            plan.update(
                                [
                                    (
                                        "Relation Name",
                                        __outer_plan["Relation Name"],
                                    )
                                ]
                            )
                        if "Schema" in __outer_plan:
                            plan.update([("Schema", __outer_plan["Schema"])])
                        if "Alias" in __outer_plan:
                            plan.update([("Alias", __outer_plan["Alias"])])
                    else:
                        if (
                            "Relation Name" in __outer_plan
                            and "Relation Name" in __inner_plan
                        ):
                            plan.update(
                                [
                                    (
                                        "Relation Name",
                                        [
                                            __outer_plan["Relation Name"],
                                            __inner_plan["Relation Name"],
                                        ],
                                    )
                                ]
                            )

                        if "Schema" in __outer_plan and "Schema" in __inner_plan:
                            plan.update(
                                [
                                    (
                                        "Schema",
                                        [
                                            __outer_plan["Schema"],
                                            __inner_plan["Schema"],
                                        ],
                                    )
                                ]
                            )

                        if "Alias" in __outer_plan and "Alias" in __inner_plan:
                            plan.update(
                                [
                                    (
                                        "Alias",
                                        [
                                            __outer_plan["Alias"],
                                            __inner_plan["Alias"],
                                        ],
                                    )
                                ]
                            )
                else:
                    if "Relation Name" in __plan:
                        plan.update([("Relation Name", __plan["Relation Name"])])
                    if "Schema" in __plan:
                        plan.update([("Schema", __plan["Schema"])])
                    if "Alias" in __plan:
                        plan.update([("Alias", __plan["Alias"])])

        def incr(plan):
            if "Node Type" in plan:
                self._count += 1

        def op(Plans):
            if isinstance(Plans, list):
                for i in range(0, len(Plans)):
                    incr(Plans[i])
                    if self._depth == self._count:
                        get_relations(Plans[i])
                        return
                    elif "Plans" in Plans[i]:
                        op(Plans[i]["Plans"])
            else:
                incr(Plans)
                if self._depth == self._count:
                    get_relations(Plans)
                    return
                elif "Plans" in Plans:
                    op(Plans["Plans"])

        self._depth = depth
        self._count = 0
        op(Plans)

    def __add_relations(self, Plans):
        """
        Add "Relation Name"
        """
        i = self.count_nodes(Plans)
        while 0 < i:
            self.__set_relations(Plans["Plan"], i)
            i -= 1

    def __regression(self, Plans, reg_param, queryid, planid):
        """
        Calculate the regression parameters of Plans, and Set the results into
        reg_param.

        Parameters
        ----------
        Plans : dict
          A plan grouped with the same queryid-planid.
        reg_param : dict
          A dict type skeleton with the same structure as Plans.
        queryid : int
        planid : int

        Returns
        -------
        reg_param: dict
          A dict which contains the regression parameter in each node.

        """

        def incr(plan):
            if "Node Type" in plan:
                self._count += 1

        def op(Plans, reg_param, queryid, planid):
            if isinstance(Plans, list):
                for i in range(0, len(Plans)):
                    incr(Plans[i])
                    self.__calc_regression(
                        Plans[i], reg_param[i], queryid, planid, self._count
                    )
                    if "Plans" in Plans[i]:
                        op(Plans[i]["Plans"], reg_param[i]["Plans"], queryid, planid)
                return
            else:
                incr(Plans)
                self.__calc_regression(Plans, reg_param, queryid, planid, self._count)
                if "Plans" in Plans:
                    op(Plans["Plans"], reg_param["Plans"], queryid, planid)
                return

        # Main procedure.
        self._count = 0
        op(Plans, reg_param, queryid, planid)

    def __get_sort_space_used(self, Plans, queryid, planid):
        """
        Get "Sort Space Used" if "Sort Space Type" is "Disk"

        Parameters
        ----------
        Plans : dict
          A plan grouped with the same queryid-planid.
        queryid : int
        planid : int

        Returns
        -------
        Max "Sort Space Used" value.

        """

        def incr(plan):
            if "Node Type" in plan:
                self._count += 1

        def pickup_sort_space_used(plan, queryid, planid):
            self.__incr_level()
            if "Sort Space Type" in plan:
                _type = plan["Sort Space Type"]
                _used = plan["Sort Space Used"]
                for i in range(len(_type)):
                    if _type[i] == "Disk":
                        if self._max_sort_space_used < _used[i]:
                            self._max_sort_space_used = _used[i]

        def op(Plans, queryid, planid):
            if isinstance(Plans, list):
                for i in range(0, len(Plans)):
                    incr(Plans[i])
                    pickup_sort_space_used(Plans[i], queryid, planid)
                    if "Plans" in Plans[i]:
                        op(Plans[i]["Plans"], queryid, planid)
                return
            else:
                incr(Plans)
                pickup_sort_space_used(Plans, queryid, planid)
                if "Plans" in Plans:
                    op(Plans["Plans"], queryid, planid)
                return

        # Main procedure.
        self._count = 0
        self._max_sort_space_used = 0
        op(Plans, queryid, planid)
        return None if self._max_sort_space_used == 0 else self._max_sort_space_used

    """
    Public method
    """

    def regression(self, serverId, work_mem=True):
        """
        Calculate the regression parameters of all serverId's query plans
        in the repository.
        """

        if self.check_serverId(serverId) == False:
            if Log.error <= self.LogLevel:
                print("Error: serverId '{}' is not registered.".format(serverId))
            sys.exit(1)

        self.__set_serverId(serverId)
        self.set_log_level(self.LogLevel)

        if Log.info <= self.LogLevel:
            print("Info: Calculating regression parameters.")

        """
        Check the grouping stat file.
        """
        _grouping_seqid = self.get_seqid_from_grouping_stat(self.ServerId)

        """
        Check the regression stat file.
        """
        self.check_regression_dir(self.ServerId)
        _regression_seqid = self.get_seqid_from_regression_stat(self.ServerId)

        if Log.debug3 <= self.LogLevel:
            print(
                "Debug3: _grouping_seqid={} _regression_seqid={}".format(
                    _grouping_seqid, _regression_seqid
                )
            )

        """
        Calculate the regression parameters.
        """
        if _regression_seqid < _grouping_seqid:

            for _hash_subdir in self.get_grouping_dir_list(self.ServerId):
                _gsdirpath = self.get_grouping_subdir_path(self.ServerId, _hash_subdir)
                if os.path.isdir(_gsdirpath):
                    _gsdirlist = self.get_grouping_subdir_list(
                        self.ServerId, _hash_subdir
                    )
                    for f in _gsdirlist:
                        _gpath = self.path(_gsdirpath, f)
                        _qp_id = str(f).split(".")
                        _queryid = _qp_id[0]
                        _planid = _qp_id[1]
                        if Log.debug3 <= self.LogLevel:
                            print("Debug3: >>>>>> gpath={}".format(_gpath))

                        _json_dict = self.read_plan_json(_gpath)
                        _reg_param = self.read_plan_json(_gpath)
                        self.__add_relations(_reg_param)
                        self.delete_unnecessary_objects(
                            self.__delete_objects, _reg_param
                        )

                        """
                        Calculate the regression parameters in each plan
                        and Store into _reg_param.
                        """
                        self.__init_level()
                        self.__regression(
                            _json_dict["Plan"], _reg_param["Plan"], _queryid, _planid
                        )

                        """
                        Add "Sort Space Used" item if "Sort Space Type" is "Disk".
                        """
                        if work_mem == True:
                            self.__init_level()
                            _max_sort_space_used = self.__get_sort_space_used(
                                _json_dict["Plan"], _queryid, _planid
                            )
                            if _max_sort_space_used is not None:
                                _reg_param.update(
                                    {"SortSpaceUsed": _max_sort_space_used}
                                )

                        """
                        Write the result (regression parameters) to the regression
                        directory.
                        """
                        _rsdirpath = self.get_regression_subdir_path(
                            self.ServerId, _hash_subdir
                        )
                        if os.path.exists(_rsdirpath) == False:
                            os.makedirs(_rsdirpath)

                        _rpath = self.path(_rsdirpath, f)
                        self.write_plan_json(_reg_param, _rpath)

                        if Log.debug3 <= self.LogLevel:
                            print("Debug3: Rpath={}".format(_rpath))
                            print("Debug3:   reg_param={}".format(_reg_param))

            """Update stat file"""
            self.update_regression_stat_file(self.ServerId, _grouping_seqid)
