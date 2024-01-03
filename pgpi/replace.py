"""
replace.py


  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 2021-2024, Hironobu Suzuki @ interdb.jp
"""

from .common import Common, Log


class Replace(Common):
    def __init__(self, log_level=Log.info):
        self.__numNode = 0
        self.__ar = []
        self.LogLevel = log_level

    def __calc(self, plan, param, queryid, planid, depth):
        def get_children_plan_rows(plan):
            _X = [[], []]
            _NP = [[], []]
            _NPP = [[], []]
            for i in range(0, 2):  # Ignore SubPlans.
                p = plan["Plans"][i]
                k = 0 if p["Parent Relationship"] == "Outer" else 1
                _X[k] = p["Plan Rows"]
                _NP[k] = p["NormalizeParam"]
                _NPP[k] = p["NormalizePlanParam"]
            return (_X[0], _X[1], _NP[0], _NP[1], _NPP[0], _NPP[1])

        def get_inputs(plan):
            # Get outer and inter plan rows.
            (
                _Xouter,
                _Xinner,
                _NPouter,
                _NPinner,
                _NPPouter,
                _NPPinner,
            ) = get_children_plan_rows(plan)
            if Log.debug3 <= self.LogLevel:
                print(
                    "Debug3: Xouter={} Xinner={}   normalize({}, {})   normalizePlanPalam({}, {})".format(
                        _Xouter, _Xinner, _NPouter, _NPinner, _NPPouter, _NPPinner
                    )
                )
            # Get Removed Rows.
            _RR = self.count_removed_rows(plan)
            return (_Xouter, _Xinner, _NPouter, _NPinner, _NPPouter, _NPPinner, _RR)

        """
        Start processing
        """
        _node_type = plan["Node Type"]

        if Log.debug1 <= self.LogLevel:
            print(
                "Debug1: count={} depth={} Node Type={}".format(
                    self._count, self._depth, plan["Node Type"]
                )
            )

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
                    _NPouter,
                    _NPinner,
                    _NPPouter,
                    _NPPinner,
                    _RR,
                ) = get_inputs(plan)
                """
                Correct Plan Rows using the regression parameter
                and the EstimatedRows of both inner and outer nodes.
                """
                if Log.debug1 <= self.LogLevel:
                    print(
                        "Debug1: Y ActualRows={}     NormalizeParam={}".format(
                            plan["Actual Rows"], plan["NormalizeParam"]
                        )
                    )
                    print(
                        "Debug1: Xouter ={}     NormalizeParam={}".format(
                            _Xouter, _NPouter
                        )
                    )
                    print(
                        "Debug1: Xinner ={}     NormalizeParam={}".format(
                            _Xinner, _NPinner
                        )
                    )

                _EstimatedRows = round(param["Coefficient"][0] * _Xouter * _Xinner)
                if Log.debug1 <= self.LogLevel:
                    print(
                        "Debug1: EstimatedRows({}) = Coef({}) * Xouter({}) * Xinner({})".format(
                            _EstimatedRows, param["Coefficient"][0], _Xouter, _Xinner
                        )
                    )

                plan.update(Coefficient=param["Coefficient"][0])
                plan.update({"Plan Rows": _EstimatedRows})
                if Log.debug1 <= self.LogLevel:
                    plan.update(OriginalPlanRows=plan["Plan Rows"])
                return

        """
        hash or merge join
        """
        for n in ("Merge Join", "Hash Join"):
            if n == _node_type:
                (
                    _Xouter,
                    _Xinner,
                    _NPouter,
                    _NPinner,
                    _NPPouter,
                    _NPPinner,
                    _RR,
                ) = get_inputs(plan)
                """
                Correct Plan Rows using the regression parameter
                and the EstimatedRows of both inner and outer nodes.
                """
                if Log.debug1 <= self.LogLevel:
                    print(
                        "Debug1: Y ActualRows={}     NormalizeParam={}".format(
                            plan["Actual Rows"], plan["NormalizeParam"]
                        )
                    )
                    print(
                        "Debug1: Xouter ={}     NormalizeParam={}".format(
                            _Xouter, _NPouter
                        )
                    )
                    print(
                        "Debug1: Xinner ={}     NormalizeParam={}".format(
                            _Xinner, _NPinner
                        )
                    )

                if param["Coefficient"][0] == 0 and param["Coefficient"][1] == 0:
                    _EstimatedRows = round(
                        param["Coefficient2"][0] * _Xouter * _Xinner
                        + param["Intercept"][0]
                    )
                    plan.update(Coefficient=[0, 0])
                    plan.update(Coefficient2=param["Coefficient2"][0])
                    plan.update(Intercept=param["Intercept"])
                    if Log.debug1 <= self.LogLevel:
                        print(
                            "Debug1: EstimatedRows({}) = Coef({}) * Xouter({}) * Xinner({}) + {}".format(
                                _EstimatedRows,
                                param["Coefficient2"][0],
                                _Xouter,
                                _Xinner,
                                param["Intercept"],
                            )
                        )
                else:
                    _EstimatedRows = round(
                        (param["Coefficient"][0] * _Xouter)
                        + (param["Coefficient"][1] * _Xinner)
                        + param["Intercept"][0]
                    )
                    plan.update(Coefficient=param["Coefficient"])
                    plan.update(Coefficient2=0)
                    plan.update(Intercept=param["Intercept"])
                    if Log.debug1 <= self.LogLevel:
                        print(
                            "Debug1: EstimatedRows({}) = Coef({}) * Xouter({}) + Coef({}) * Xinner({}) + {}".format(
                                _EstimatedRows,
                                param["Coefficient"][0],
                                _Xouter,
                                param["Coefficient"][1],
                                _Xinner,
                                param["Intercept"],
                            )
                        )

                plan.update({"Plan Rows": _EstimatedRows})
                if Log.debug1 <= self.LogLevel:
                    plan.update(OriginalPlanRows=plan["Plan Rows"])
                return

        """
        scan type
        """

        """Calculate 'EstimatedRows' using the regression parameter."""
        if "Plan Rows" in plan:
            _EstimatedRows = (
                param["Coefficient"][0] * plan["Plan Rows"] + param["Intercept"][0]
            )
            _EstimatedRows = round(
                _EstimatedRows * plan["NormalizePlanParam"] / plan["NormalizeParam"]
            )
            if Log.debug1 <= self.LogLevel:
                print(
                    "Debug1: EstimatedRows({}) = [Coef({}) * PlanRows({}) + Intercept({})] *  NormalizePlan({}) / Normalize({})".format(
                        _EstimatedRows,
                        param["Coefficient"][0],
                        plan["Plan Rows"],
                        param["Intercept"][0],
                        plan["NormalizePlanParam"],
                        plan["NormalizeParam"],
                    )
                )

            plan.update(Coefficient=param["Coefficient"][0])
            plan.update({"Plan Rows": _EstimatedRows})
            if Log.debug1 <= self.LogLevel:
                plan.update(OriginalPlanRows=plan["Plan Rows"])
        return

    def __replace(self, Plans, Reg_Params, depth, queryid, planid):
        """
        In the depth-th node of Plans, Replace the 'Plan Rows' of original Plans
        with the estimated 'Plan Rows' using the regression params:Reg_Params.
        """

        def incr(plan):
            if "Node Type" in plan:
                self._count += 1

        def op(Plans, Reg_Params, queryid, planid):
            if isinstance(Plans, list):
                for i in range(0, len(Plans)):
                    incr(Plans[i])
                    if self._depth == self._count:
                        self.__calc(
                            Plans[i], Reg_Params[i], queryid, planid, self._count
                        )
                        return
                    elif "Plans" in Plans[i]:
                        op(Plans[i]["Plans"], Reg_Params[i]["Plans"], queryid, planid)
            else:
                incr(Plans)
                if self._depth == self._count:
                    self.__calc(Plans, Reg_Params, queryid, planid, self._count)
                    return
                elif "Plans" in Plans:
                    op(Plans["Plans"], Reg_Params["Plans"], queryid, planid)

        self._depth = depth
        self._count = 0
        op(Plans, Reg_Params, queryid, planid)

    """
    Public method
    """

    def replace_plan_rows(self, Plans, Reg_Params, numNode, queryid, planid):
        """
        Replace the 'Plan Rows' of original Plans with the estimated 'Plan Rows'
        using the regression parameters:Reg_Params.

        Parameters
        ----------
        Plans : dict
          A Plans

        Reg_Params : dict
          A dict that stores the regression parameters of Plans.

        numNode : int
          The number of nodes of Plans.

        queryid : int
        planid : int
        """

        """
        'Plan Rows' of the Plans are estimated and replaced from the bottom
        of the Plans to the top.

        If the node type is like Scan, e.g., Seq Scan and Index Only Scan,
        the 'Plan Rows' is replaced with the estimated 'Plan Rows' which is
        calculated using the corresponding regression parameters.

        Otherwise, that is, the node type is like Join, e.g., Nested Loop and
        Hash Join, the 'Plan Rows' is calculated using the corresponding regression
        parameters and the estimated 'Plan Rows' of both inner and outer nodes.

        The simplest example is shown below:

        This example plan is a nested loop join. The outer and inner nodes of
        the nested loop are Seq Scan and Index Only Scan, respectively.
        The 'Plan Rows' generated by the optimizer and the regression parameters
        created by repo_mgr.py of all nodes are shown below:

        ```
        Nested Loop                  (Plan Rows =   1)        (Reg_Params[Node1][coef] = 0.05)
          ->  Outer:Seq Scan         (Plan Rows =   3)        (Reg_Params[Node2][coef] =  2.0)
          ->  Inner:Index Only Scan  (Plan Rows =  10)        (Reg_Params[Node3][coef] =  1.0)
        ```

        In the following, how to replace 'Plan Rows' are shown.

        (1) Estimate the inner rows.
        The estimated inner rows can be calculated as follows:
         estimated inner Plan Rows = Reg_Params[Node3][coef] * Inner Plan Rows
                                   = 1.0 * 10
                                   = 10
        In this case, the Plan Rows and the estimated Plan Rows are equal, so no need to replace.
        ```
        Nested Loop                  (Plan Rows =   1)
          ->  Outer:Seq Scan         (Plan Rows =   3)
          ->  Inner:Index Only Scan  (Plan Rows =  10)        (Reg_Params[Node3][coef] =  1.0)
        ```

        (2) Estimate the outer rows.
        The estimated outer row can be calculated as follows:
         estimated outer Plan Rows = Reg_Params[Node2][coef] * Inner Plan Rows
                                   = 2.0 * 3
                                   = 6
        Then, replace the original 'Plan Rows' with the estimated one.
        ```
        Nested Loop                  (Plan Rows =   1)
          ->  Outer:Seq Scan         (Plan Rows =   6)        (Reg_Params[Node2][coef] =  2.0)
          ->  Inner:Index Only Scan  (Plan Rows =  10)
        ```

        (3) Estimate the nested loop's plan rows.
        The estimated nested loop's rows can be calculated as follows:
         estimated nested loop's Rows = Reg_Params[Node1][coef] * 'estimated outer Plan Rows' * 'estimated inner Plan Rows'
                                      = 0.05 * 6 * 10
                                      = 3
        Then, replace the original 'Plan Rows' with the estimated one.
        ```
        Nested Loop                  (Plan Rows =   3)        (Reg_Params[Node1][coef] = 0.05)
          ->  Outer:Seq Scan         (Plan Rows =   6)
          ->  Inner:Index Only Scan  (Plan Rows =  10)
        ```
        """

        i = numNode
        if Log.debug1 <= self.LogLevel:
            print("Debug1: >>> Start replace")
        while 0 < i:
            if Log.debug1 <= self.LogLevel:
                print("Debug1: >>> replace i = {}".format(i))
            self.__replace(Plans, Reg_Params, i, queryid, planid)
            i -= 1
