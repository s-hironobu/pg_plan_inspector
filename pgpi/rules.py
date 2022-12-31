"""
rules.py

  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 20212-2023, Hironobu Suzuki @ interdb.jp
"""

from .common import Common, State, Log


class Rules(Common):
    def __init__(self, log_level=Log.error):
        self.LogLevel = log_level

    """
    All rules are heuristics; there is no theoretical background.
    """

    def __rule1(self, plan):
        if (
            plan["CurrentState"] == State.RUNNING
            and plan["Node Type"] == "Hash Join"
            and "Join Filter" in plan
        ):
            # if plan["Plan Rows"] == plan["Actual Rows"]:
            if plan["Plan Rows"] <= plan["Actual Rows"]:
                for p in plan["Plans"]:
                    p["CurrentState"] = State.FINISHED

    def __rule2(self, plan):
        if (
            plan["CurrentState"] == State.RUNNING
            and plan["Node Type"] == "Materialize"
            or plan["Node Type"] == "Hash"
        ) and (
            plan["Actual Loops"] > 0
            or plan["Actual Rows"] > 0
            or plan["MergeFlag"] == "True"
        ):
            plan["CurrentState"] = State.FINISHED

    def __rule3(self, plan):
        if (
            plan["CurrentState"] == State.RUNNING
            and self.isScan(plan)
            and (self.isInner(plan) and plan["Actual Loops"] > 0)
        ):
            plan["CurrentState"] = State.FINISHED

    def __rule4(self, plan):
        if (
            plan["CurrentState"] == State.RUNNING
            and self.isScan(plan)
            and self.isOuter(plan)
            and (plan["Plan Rows"] <= plan["Actual Rows"])
        ):
            plan["CurrentState"] = State.FINISHED

    def __rule5(self, plan):
        if (
            plan["CurrentState"] == State.RUNNING
            and self.isScan(plan)
            and (self.isOuter(plan) == False and self.isInner(plan) == False)
        ):
            plan["CurrentState"] = State.FINISHED

    def __rule6(self, plan):
        if (
            plan["CurrentState"] == State.RUNNING
            and (plan["Node Type"] == "Hash Join" or plan["Node Type"] == "Merge Join")
            and "Join Filter" not in plan
        ):
            # Magic rule
            if plan["Plan Rows"] * 5 < plan["Actual Rows"]:
                for p in plan["Plans"]:
                    if p["Parent Relationship"] == "Outer":
                        plan["Plan Rows"] = p["Plan Rows"]

    # Apply the rules recursively
    def __op(self, Plans):
        if isinstance(Plans, list):
            for plan in Plans:
                for r in self.rules:
                    r(plan)
                if "Plans" in plan:
                    self.__op(plan["Plans"])
            return
        else:
            for r in self.rules:
                r(Plans)
            if "Plans" in Plans:
                self.__op(Plans["Plans"])
            return

    """
    Public method
    """

    def apply_rules(self, plans):
        """
        Apply the rules to 'plans' if there is no regression params.
        (if there are regression params, this method is skipped in query_progress.py.)

        You can add or remove rules to suit your environment.
        """

        self.rules = [
            self.__rule1,
            self.__rule2,
            self.__rule3,
            self.__rule4,
            self.__rule5,
            self.__rule6,
        ]
        self.__op(plans)
