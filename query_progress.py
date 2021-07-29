#!/usr/bin/env python3
"""

This script shows the actual state and progress of the running query.


Usage:
 query_progress.py [--host XXX] [--port NNN] [--dbname XXX] [--username XXX] [--password] [--verbose] [--pid NNN]
 query_progress.py [--basedir XXX] [--verbose] [--pid NNN] --serverid XXX


  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 2021, Hironobu Suzuki @ interdb.jp
"""

import psycopg2
import argparse
import getpass
import sys
import os
import hashlib
import re
import math

from pgpi import Database, Repository, QueryProgress, Log

if __name__ == "__main__":

    def usage():
        _msg = "Usage:\n"
        _msg += "\tquit\t\t: Quit this program\n"
        _msg += "\tv, verbose\t: Switch Verbose mode <--> Normal mode\n"
        _msg += "\th, help\t\t: Show help message\n"
        print(_msg)

    def clear_console():
        if os.name == "nt":
            os.system("cls")
        else:
            os.system("clear")

    LOG_LEVEL = Log.info
    explain_stmt = re.compile(r"^explain")

    # Parse arguments.
    parser = argparse.ArgumentParser(
        description="This script shows the actual state and progress of the running query.",
        add_help=False,
    )

    parser.add_argument(
        "--host",
        "-h",
        help='Database server host or socker directory (default: "localhost")',
        default="localhost",
    )
    parser.add_argument(
        "--port", "-p", help='Database server port (default: "5432")', default="5432"
    )
    parser.add_argument(
        "--dbname", help='Database (default: "postgres")', default="postgres"
    )
    parser.add_argument(
        "--username",
        "-U",
        help='Database user name (default: "postgres")',
        default="postgres",
    )
    parser.add_argument("--password", "-W", action="store_true", help="input password")
    parser.add_argument(
        "--serverid",
        help="Identifier of the database server you connect, which is described in hosts.conf",
        default=None,
    )
    parser.add_argument(
        "--basedir",
        help="Directory of the repository you use (default: '.')",
        default=".",
    )
    parser.add_argument(
        "--pid",
        help="Pid of the Postgres process that you want to show the query progress",
        default="0",
    )
    parser.add_argument("--verbose", action="store_true", help="Show all")
    parser._add_action(
        argparse._HelpAction(
            option_strings=["--help", "-H"], help="Show this help message and exit"
        )
    )

    args = parser.parse_args()

    """
    Make connection parameter.
    """
    base_dir = str(args.basedir)
    if args.serverid is not None:
        server_id = str(args.serverid)
        db = Database(base_dir, LOG_LEVEL)
        conn = db.get_connection_param(str(args.serverid))
        del db
    else:
        server_id = None
        conn = (
            "host="
            + str(args.host)
            + " port="
            + str(args.port)
            + " dbname="
            + str(args.dbname)
            + " user="
            + str(args.username)
        )
        if args.password == True:
            _password = getpass.getpass()
            conn += " password=" + _password

    """
    Connect to database server.
    """
    try:
        connection = psycopg2.connect(conn)
    except psycopg2.OperationalError as e:
        print("Could not connect to {}".format(args.host))
        sys.exit(1)

    connection.autocommit = True

    clear_console()

    """
    Welcome message.
    """
    print("Connected to {}".format(args.host))
    print('\n\tEnter "quit" or Contrl-C to quit this script')
    print('\tEnter "help" to show usage.\n')
    pid = args.pid
    previous_pid = pid

    verbose = args.verbose

    qp = QueryProgress(base_dir, LOG_LEVEL)

    """
    Main loop.
    """
    while True:

        # Wait for input.
        msg = "Enter pid (default: " + str(pid) + ') or "quit":'
        try:
            pid = input(msg)
        except KeyboardInterrupt:
            connection.close()
            print("\n")
            sys.exit(1)
        if pid == "quit":
            break
        elif pid == "":
            pid = previous_pid
        elif str.lower(pid) == "h" or str.lower(pid) == "help":
            usage()
            pid = previous_pid
            continue
        elif str.lower(pid) == "v" or str.lower(pid) == "verbose":
            verbose ^= True
            print(
                "Switch to {} mode".format(
                    (lambda v: "Verbose" if verbose else "Normal")(verbose)
                )
            )
            pid = previous_pid
            continue
        elif pid.isdigit() == False:
            pid = previous_pid
            continue

        clear_console()

        # Execute pg_query_plan().
        sql = "SELECT pid, database, worker_type, nested_level, queryid, query, plan, plan_json"
        sql += " FROM pg_query_plan(" + pid + ")"
        cur = connection.cursor()
        try:
            cur.execute(sql)
        except Exception as err:
            cur.close()
            print("Error! Check the pid:{} you set.".format(pid))
            continue

        if cur.rowcount == 0:
            cur.close()
            print("retuned 0 row")
            previous_pid = pid
            continue

        # Prepare data to calcurate the progress of the queries.
        _X = []
        for row in cur:
            _worker_type = row[2]
            _queryid = int(row[4])
            _query = row[5]
            _plan_json = row[7]
            _planid = qp.calc_planId(_plan_json)
            if Log.debug1 <= LOG_LEVEL:
                print("Debug1: queryid={}  planid={}".format(_queryid, _planid))
            _X.append(
                [
                    _worker_type,
                    _queryid,
                    _planid,
                    _plan_json,
                    int(
                        hashlib.md5(str(_query).encode()).hexdigest(), 16
                    ),  # For version 13.
                ]
            )

        # Show the progress of the queries if the queries are NOT EXPLAIN.
        if re.match(explain_stmt, str.lower(_query)) is None:
            _ret = qp.query_progress(_X, server_id)
            print("==> Query Progress:")
            for _p in _ret:
                print("\tqueryid = {}".format(str(_p[0])))
                _percent = _p[1] * 100
                _percent = math.floor(_percent * 10 ** 2) / (10 ** 2)
                _pb = qp.make_progress_bar(_percent)
                print("\t{:>8.2f}[%]  {}".format(float(_percent), str(_pb)))
        else:
            print("Notice: EXPLAIN ANALYZE statement is out of scope.")

        # Display query info.
        cur.scroll(0, mode="absolute")
        i = 1
        for row in cur:
            _pid = row[0]
            _database = row[1]
            _worker_type = row[2]
            _nested_level = row[3]
            _queryid = int(row[4])
            _query = row[5]
            _plan = row[6]
            _plan_json = row[7]

            print("[{}]----------------------------------------------------".format(i))
            print("pid           :{}".format(_pid))
            if verbose:
                print("database      :{}".format(_database))
            print("worker_type   :{}".format(_worker_type))
            print("nested_level  :{}".format(_nested_level))
            print("queryid       :{}".format(_queryid))
            print("query         :\n               {}".format(_query))
            if verbose:
                print("query_plan    :\n{}".format(_plan))
            i += 1

        cur.close()
        previous_pid = pid

    """
    Finish processing.
    """
    connection.close()
    del qp
