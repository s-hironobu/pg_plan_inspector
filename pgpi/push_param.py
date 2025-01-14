"""
push_param.py

  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 2021-2025, Hironobu Suzuki @ interdb.jp
"""

import sys
import csv
import psycopg2
from six import string_types

from .common import Common, Log
from .database import Database
from .repository import Repository


class PushParam(Database, Repository):
    def __init__(self, base_dir=".", log_level=Log.info):
        self.set_base_dir(base_dir)
        self.LogLevel = log_level

    def __trans(self, Plans, depth):
        def merge(schemas, relations):
            result = []
            if type(schemas) is not list:
                schemas = [schemas]
            if type(relations) is not list:
                relations = [relations]
            for i in range(0, len(schemas)):
                result.append(str(schemas[i]) + "." + str(relations[i]))
            return result

        def flatten(lists):
            result = []
            if type(lists) is not list:
                lists = [lists]
            for i in lists:
                if isinstance(i, string_types):
                    result.append(i)
                else:
                    result.extend(flatten(i))
            return result

        def get_items(plan):
            """

            Format:  { "Node Type" : ("rtable" [, ...]) :  ("outer_rtable" [, ...]) : ("inner_rtable" [, ...]) : [Coeffcient [, ...]]  : [Coefficient2] : [Intercept] : "MergeFlag"}

            """

            _visible = False
            _items = "{"
            if "Node Type" in plan:
                if (
                    plan["Node Type"] == "Nested Loop"
                    or plan["Node Type"] == "Merge Join"
                    or plan["Node Type"] == "Hash Join"
                ):
                    _visible = True
                _items = _items + "'" + str(plan["Node Type"]) + "':"
                if Log.debug3 <= self.LogLevel:
                    print("Debug3:    Node Type={}".format(plan["Node Type"]))
            if "Schema" in plan and "Relation Name" in plan:
                if Log.debug3 <= self.LogLevel:
                    print(
                        "Debug3:    rtable={}".format(
                            merge(
                                flatten(plan["Schema"]), flatten(plan["Relation Name"])
                            )
                        )
                    )
                _items = (
                    _items
                    + str(
                        str(
                            merge(
                                flatten(plan["Schema"]), flatten(plan["Relation Name"])
                            )
                        ).replace("[", "(")
                    ).replace("]", ")")
                    + ":"
                )
            if "Plans" in plan:
                __plan = plan["Plans"]
                if isinstance(__plan, list):
                    __outer_plan = __plan[0]
                    __inner_plan = __plan[1] if 2 <= len(__plan) else None

                    if "Schema" in __outer_plan and "Relation Name" in __outer_plan:
                        if Log.debug3 <= self.LogLevel:
                            print(
                                "Debug3:       outer_rtable={}".format(
                                    merge(
                                        flatten(__outer_plan["Schema"]),
                                        flatten(__outer_plan["Relation Name"]),
                                    )
                                )
                            )
                        _items = (
                            _items
                            + str(
                                str(
                                    merge(
                                        flatten(__outer_plan["Schema"]),
                                        flatten(__outer_plan["Relation Name"]),
                                    )
                                ).replace("[", "(")
                            ).replace("]", ")")
                            + ":"
                        )

                    if (
                        __inner_plan is not None
                        and "Schema" in __inner_plan
                        and "Relation Name" in __inner_plan
                    ):
                        if Log.debug3 <= self.LogLevel:
                            print(
                                "Debug3:       inner_rtable={}".format(
                                    merge(
                                        flatten(__inner_plan["Schema"]),
                                        flatten(__inner_plan["Relation Name"]),
                                    )
                                )
                            )
                        _items = (
                            _items
                            + str(
                                str(
                                    merge(
                                        flatten(__inner_plan["Schema"]),
                                        flatten(__inner_plan["Relation Name"]),
                                    )
                                ).replace("[", "(")
                            ).replace("]", ")")
                            + ":"
                        )
                    elif __inner_plan is None:
                        if Log.debug3 <= self.LogLevel:
                            print("Debug3:       inner_rtable=NULL")
                else:
                    if "Schema" in __plan and "Relation Name" in __plan:
                        if Log.debug3 <= self.LogLevel:
                            print(
                                "Debug3:       outer_rtable={}".format(
                                    merge(
                                        flatten(__plan["Schema"]),
                                        flatten(__plan["Relation Name"]),
                                    )
                                )
                            )
                            print("Debug3:       inner_rtable=NULL")
            else:
                if Log.debug3 <= self.LogLevel:
                    print("Debug3:       outer_rtable=NULL, inner_rtable=NULL")
                _items = _items + "():():"
                _visible = True

            if "Coefficient" in plan:
                if Log.debug3 <= self.LogLevel:
                    print("Debug3:    Coefficient={}".format(plan["Coefficient"]))
                _items = _items + str(plan["Coefficient"]) + ":"
            if "Coefficient2" in plan:
                if Log.debug3 <= self.LogLevel:
                    print("Debug3:    Coefficient2={}".format(plan["Coefficient2"]))
                _items = _items + str(plan["Coefficient2"]) + ":"
            else:
                _items = _items + "[]:"
            if "Intercept" in plan:
                if Log.debug3 <= self.LogLevel:
                    print("Debug3:    Intercept={}".format(plan["Intercept"]))
                _items = _items + str(plan["Intercept"]) + ":"
            else:
                _items = _items + "[]:"

            if "MergeFlag" in plan:
                if Log.debug3 <= self.LogLevel:
                    print("Debug3:    MergeFlag={}".format(plan["MergeFlag"]))
                _items = _items + '"' + str(plan["MergeFlag"]) + '"'
            else:
                _items = _items + "[]"

            _items = _items + "}"

            if _visible == True:
                if Log.debug1 <= self.LogLevel:
                    print("Debug1: items={}".format(_items))
                return _items
            else:
                return None

        def incr(plan):
            if "Node Type" in plan:
                self._count += 1

        def op(Plans):
            if isinstance(Plans, list):
                for i in range(0, len(Plans)):
                    incr(Plans[i])
                    if self._depth == self._count:
                        self._params = get_items(Plans[i])
                        return
                    elif "Plans" in Plans[i]:
                        op(Plans[i]["Plans"])
            else:
                incr(Plans)
                if self._depth == self._count:
                    self._params = get_items(Plans)
                    return
                elif "Plans" in Plans:
                    op(Plans["Plans"])

        self._depth = depth
        self._count = 0
        self._params = ""
        op(Plans)
        return self._params

    def __transform(self, reg_path):

        _result = ""
        i = self.count_nodes(reg_path)
        while 0 < i:
            _params = self.__trans(reg_path["Plan"], i)
            if _params != None:
                if not _result:
                    _result = _params
                else:
                    _result = _result + ";" + _params
            i -= 1
        return _result

    def __get_database_list(self, serverId):
        """Get database list."""
        _ret = []
        with open(self.get_log_csv_path(serverId), newline="") as f:
            _reader = csv.reader(f, delimiter=",", quoting=csv.QUOTE_NONE)
            for _row in _reader:
                _database = _row[3]
                if _database not in _ret:
                    _ret.append(_database)
        return _ret

    def __get_queryid_and_param_list(self, serverId, db):
        _dict = {}
        with open(self.get_log_csv_path(serverId), newline="") as f:
            _reader = csv.reader(f, delimiter=",", quoting=csv.QUOTE_NONE)
            for _row in _reader:
                _seqid = int(_row[0])
                _database = _row[3]
                _queryid = int(_row[6])
                _planid = int(_row[7])
                if _database == db:
                    _dict[_queryid] = str(_planid)
        return _dict

    def __check_object(self, connection, sql):
        _cur = connection.cursor()
        try:
            _cur.execute(sql)
        except Exception as err:
            _cur.close()
            print("Error! Could not execute sql:{}.".format(sql))
            sys.exit(1)

        if _cur.rowcount != 1:
            _cur.close()
            print("Error! Got invalid result.")
            sys.exit(1)

        for _row in _cur:
            _num = _row[0]
            _cur.close()
            return True if _num == 1 else False
        return False

    def __check_schema(self, connection):
        _sql = (
            "SELECT count(*) FROM pg_namespace WHERE nspname = '" + self.SCHEMA + "';"
        )
        return self.__check_object(connection, _sql)

    def __check_table(self, connection):
        _sql = (
            "SELECT count(*) FROM pg_class AS c, pg_namespace AS n WHERE n.oid = c.relnamespace AND c.relname = "
            + "'"
            + self.REG_PARAMS_TABLE
            + "'"
            + " AND n.nspname = "
            + "'"
            + self.SCHEMA
            + "'"
            + ";"
        )
        return self.__check_object(connection, _sql)

    def __execute_sql(self, connection, sql):
        _cur = connection.cursor()
        try:
            _cur.execute(sql)
        except Exception as err:
            _cur.close()
            print("Error! Could not execute sql:{}.".format(sql))
            sys.exit(1)

    def __create_schema(self, connection):
        _sql = "CREATE SCHEMA IF NOT EXISTS " + self.SCHEMA + ";"
        self.__execute_sql(connection, _sql)

    def __create_table(self, connection, work_mem):
        _sql = (
            "CREATE TABLE IF NOT EXISTS "
            + self.SCHEMA
            + "."
            + self.REG_PARAMS_TABLE
            + " ("
        )
        _sql += "queryid TEXT PRIMARY KEY,"
        if work_mem == True:
            _sql += "sort_space_used INT,"
        _sql += "params TEXT NOT NULL"
        _sql += ");"
        self.__execute_sql(connection, _sql)

    def __truncate_table(self, connection):
        _sql = "TRUNCATE " + self.SCHEMA + "." + self.REG_PARAMS_TABLE + ";"
        self.__execute_sql(connection, _sql)

    def __insert_reg_params(self, connection, serverId, queryid_list, work_mem):
        _cur = connection.cursor()
        try:
            _cur.execute("START TRANSACTION;")
        except Exception as err:
            _cur.close()
            print("Error! Could not execute sql:{}.".format("START TRANSACTION"))
            sys.exit(1)

        for _queryid in queryid_list:
            _planid = int(queryid_list[_queryid])
            _reg_path = self.get_regression_param(serverId, _queryid, _planid)

            _result = self.__transform(_reg_path)
            _sort_space_used = None
            if work_mem == True:
                if "SortSpaceUsed" in _reg_path:
                    _sort_space_used = _reg_path["SortSpaceUsed"]

            if Log.debug3 <= self.LogLevel:
                print(
                    "Debug3: _queryid={}  planid={}  _result={}".format(
                        _queryid, queryid_list[_queryid], _result
                    )
                )

            _sql = "INSERT INTO " + self.SCHEMA + "." + self.REG_PARAMS_TABLE

            if _sort_space_used is None:
                _sql += " (queryid, params) VALUES ("
            else:
                _sql += " (queryid, sort_space_used, params) VALUES ("
            _sql += "'" + str(_queryid) + "',"
            if _sort_space_used is not None:
                _sql += str(int(_sort_space_used)) + ","
            _sql += "'" + str(_result).replace("'", '"') + "'"
            _sql += ");"

            try:
                _cur.execute(_sql)
            except Exception as err:
                _cur.close()
                print("Error! Could not execute sql:{}.".format(_sql))
                sys.exit(1)

            # Write formatted reg param file
            self.write_formatted_regression_params(
                serverId, str(_queryid), str(_result).replace("'", '"')
            )

        try:
            _cur.execute("COMMIT;")
        except Exception as err:
            _cur.close()
            print("Error! Could not execute sql:{}.".format("COMMIT"))
            sys.exit(1)
        _cur.close()

    """
    Public method
    """

    def push_param(self, serverId, work_mem=True):
        """
        Check formatted regression params subdir, and create it if not exists.
        """
        self.check_formatted_regression_params_dir(serverId)

        """
        Get database list from log.csv.
        """
        _database_list = self.__get_database_list(serverId)
        if len(_database_list) == 0:
            if Log.info <= self.LogLevel:
                print("Info: There is no data.")
            sys.exit(0)

        if Log.debug3 <= self.LogLevel:
            print("Debug3: _database_list={}".format(_database_list))

        """
        Main loop
        """
        db = Database(self.base_dir, self.LogLevel)
        for _db in _database_list:
            """
            Get queryid and its latest param list in each database.
            """
            _queryid_list = self.__get_queryid_and_param_list(serverId, _db)

            if Log.debug3 <= self.LogLevel:
                print("Debug3: queryid_list={}".format(_queryid_list))

            """
            Get connection param of each database
            """
            _conn = db.get_connection_param(serverId, _db)

            """
            Connect to each database
            """
            try:
                _connection = psycopg2.connect(_conn)
            except psycopg2.OperationalError as e:
                print("Could not connect to {}".format(_db))
                continue
            _connection.autocommit = True

            """
            Check schema and table; create them if necessary
            """
            if self.__check_schema(_connection) == False:
                self.__create_schema(_connection)

            if self.__check_table(_connection):
                self.__truncate_table(_connection)
            else:
                self.__create_table(_connection, work_mem)

            """
            Insert reg_param
            """
            self.truncate_formatted_regression_params(serverId)
            self.__insert_reg_params(_connection, serverId, _queryid_list, work_mem)

            _connection.close()

        del db
