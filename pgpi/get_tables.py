"""
get_tables.py



  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 2021-2023, Hironobu Suzuki @ interdb.jp
"""

import configparser
import os
import sys

from .common import Common, Log
from .database import Database
from .merge_plan import MergePlan
from .repository import Repository


class GetTables(MergePlan):
    def __init__(self, base_dir=".", log_level=Log.info):
        self.set_base_dir(base_dir)
        self.ServerId = ""
        self.LogLevel = log_level

    def __set_serverId(self, serverId):
        self.ServerId = serverId

    def __exec_select_cmd(self, connection, sql):
        _cur = connection.cursor()
        try:
            _cur.execute(sql)
        except Exception as err:
            _cur.close()
            if Log.error <= self.LogLevel:
                print("SQL Error:'{}'".format(sql))
            sys.exit(1)
        if _cur.rowcount == 0:
            _cur.close()
            if Log.info <= self.LogLevel:
                print("Info: the number of result is 0:'{}'".format(sql))
            return None
        return _cur

    def __get_max_seqid(self, connection):
        _max_seqid = -1
        _sql = "SELECT max(seqid) FROM " + self.SCHEMA + "." + self.LOG_TABLE
        _cur = self.__exec_select_cmd(connection, _sql)
        for _row in _cur:
            _max_seqid = _row[0]
        _cur.close()
        return _max_seqid if isinstance(_max_seqid, int) == True else 0

    def __get_log(self, connection, current_seqid, max_seqid):
        """
        This function performs the main processing of public method get_tables().
        That is, it gets new log data from query_plan.log table, and stores
        the getting data into appropriate directories.
        """

        def store_log(seqid, dirpath, data, postfix=""):
            if os.path.exists(dirpath) == False:
                os.makedirs(dirpath)
            _path = self.path(dirpath, str(seqid) + postfix)
            _qfp = open(_path, mode="w")
            _qfp.write("{}".format(data))
            _qfp.close()

        if current_seqid >= max_seqid:
            return 0

        _sql = "SELECT seqid, starttime, endtime, database, pid,"
        _sql += " nested_level, queryid, query, planid, plan, plan_json"
        _sql += "   FROM " + self.SCHEMA + "." + self.LOG_TABLE
        _sql += "     WHERE " + str(current_seqid) + " < seqid AND seqid <= "
        _sql += str(max_seqid) + " ORDER BY seqid"
        _cur = self.__exec_select_cmd(connection, _sql)
        if _cur is None:
            return 0
        _num_rows = _cur.rowcount

        _logfp = open(self.get_log_csv_path(self.ServerId), mode="a")
        for _row in _cur:
            _seqid = _row[0]
            _starttime = _row[1]
            _endtime = _row[2]
            _database = _row[3]
            _pid = _row[4]
            _nested_level = _row[5]
            _queryid = int(_row[6])
            _query = _row[7]
            if _row[8] is not None:
                _planid = int(_row[8])
            else:
                continue
            _plan = _row[9]
            _plan_json = _row[10]

            # Write query info into log.csv.
            _logfp.write(
                "{},{},{},{},{},{},{},{}\n".format(
                    _seqid,
                    _starttime,
                    _endtime,
                    _database,
                    _pid,
                    _nested_level,
                    _queryid,
                    _planid,
                )
            )

            """Store query."""
            store_log(_seqid, self.get_query_dir_path(self.ServerId, _queryid), _query)
            """Store plan."""
            store_log(
                _seqid, self.get_plan_dir_path(self.ServerId, _queryid, _planid), _plan
            )
            """Store plan_json."""
            store_log(
                _seqid,
                self.get_plan_json_dir_path(self.ServerId, _queryid, _planid),
                _plan_json,
                ".tmp",
            )

        _logfp.close()
        _cur.close()

        return _num_rows

    """
    Public method
    """

    def get_tables(self, serverId):
        """
        Get new log data from query_plan.log table, and store the data
        into appropriate directories.

        Parameters
        ----------
        serverId : str
          The serverId of the database server that is described in the hosts.conf.

        Returns
        -------
        _num_rows : int
           Return the number of rows of the getting result;
           return 0 if there is no new rows.
        """

        if self.check_serverId(serverId) == False:
            if Log.error <= self.LogLevel:
                print("Error: serverId '{}' is not registered.".format(serverId))
            sys.exit(1)

        self.__set_serverId(serverId)

        """Connect to DB server."""
        db = Database(self.base_dir)
        _conn = db.connect(serverId)
        if Log.info <= self.LogLevel:
            print("Info: Connection established to '{}'.".format(self.ServerId))
        """Get max seqid."""
        _max_seqid = self.__get_max_seqid(_conn)

        """Get current seqid."""
        self.check_tables_dir(serverId)
        _current_seqid = self.get_seqid_from_tables_stat(serverId)

        """Get data from log table and write to tables directory."""
        if Log.info <= self.LogLevel:
            print("Info: Getting query_plan.log table data.")
        _num_rows = self.__get_log(_conn, _current_seqid, _max_seqid)
        if _num_rows > 0:
            self.add_workers_rows(serverId, _current_seqid, _max_seqid)
            """Update the stat file."""
            self.update_tables_stat_file(serverId, _max_seqid)
        else:
            if Log.info <= self.LogLevel:
                print("Info: data is already updated.")

        """Commit transaction and Close database connection."""
        _conn.commit()
        _conn.close()
        if Log.info <= self.LogLevel:
            print("Info: Connection closed.")

        del db

        return _num_rows
