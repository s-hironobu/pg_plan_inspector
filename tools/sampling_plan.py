#!/usr/bin/env python3
"""
A debugging script for query_progress methods.

Usage:
   sampling_plan.py sampling [--host XXX] [--port NNN]
                                 [--dbname XXX] [--username XXX]
                                 [--password XXX] [--pid NNN]
                                 [--prefix XXX] [--time NNN]

   sampling_plan.py check [--basedir XXX] [--serverid XXX]
                              [--prefix XXX] [--no NNN]


  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 2021-2022, Hironobu Suzuki @ interdb.jp
"""

import psycopg2
import argparse
import json
import sys
import os
import time

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from pgpi import *

if __name__ == "__main__":

    # Functions
    def sampling(args):
        # Make connection parameter
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
        if args.password is not None:
            conn += " password=" + str(args.password)

        # Connect to database server
        try:
            connection = psycopg2.connect(conn)
        except psycopg2.OperationalError as e:
            print("Could not connect to {}".format(args.host))
            sys.exit(1)

        connection.autocommit = True

        # Welcome message
        print("Connected to {}".format(args.host))

        sleeptime = int(args.time)
        pid = args.pid

        sql = (
            "SELECT pid, database, worker_type, nested_level, queryid, query, planid, plan, plan_json FROM pg_query_plan("
            + pid
            + ")"
        )

        count = 0

        c = Common()

        # Main loop
        while True:

            # Wait sleeptime [sec]
            time.sleep(sleeptime)

            # Execute Query
            cur = connection.cursor()
            try:
                cur.execute(sql)
            except Exception as err:
                cur.close()
                print("Error!")
                continue

            # Show result
            if cur.rowcount == 0:
                cur.close()
                print("retuned 0 row")
                continue

            count += 1
            worker_no = 1
            i = 1

            for row in cur:
                _pid = row[0]
                _database = row[1]
                _worker_type = row[2]
                _nested_level = row[3]
                _queryid = row[4]
                _query = row[5]
                _planid = int(row[6])
                _plan = row[7]
                _plan_json = row[8]

                print(
                    "[{}]----------------------------------------------------".format(i)
                )
                print("pid            :{}".format(_pid))
                print("database       :{}".format(_database))
                print("worker_type    :{}".format(_worker_type))
                print("nested_level   :{}".format(_nested_level))
                print("queryid        :{}".format(_queryid))
                print("query          :\n{}".format(_query))
                print("planid         :{}".format(_planid))
                print("plan           :\n{}".format(_plan))
                i += 1

                if _worker_type == "leader":
                    if count == 1:
                        fp = open(args.prefix + ".queryid", mode="w")
                        fp.write("{}".format(_queryid))
                        fp.close()
                        fp = open(args.prefix + ".planid", mode="w")
                        fp.write("{}".format(_planid))
                        fp.close()
                        fp = open(args.prefix + ".query", mode="w")
                        fp.write("{}".format(_query))
                        fp.close()

                    fp = open(
                        args.prefix + "-plan-" + str(count).zfill(3) + ".0", mode="w"
                    )
                    fp.write("{}".format(_plan))
                    fp.close()
                    fp = open(
                        args.prefix + "-plan-json-" + str(count).zfill(3) + ".0",
                        mode="w",
                    )
                    fp.write("{}".format(_plan_json))
                    fp.close()
                else:
                    fp = open(
                        args.prefix
                        + "-plan-"
                        + str(count).zfill(3)
                        + "."
                        + str(worker_no),
                        mode="w",
                    )
                    fp.write("{}".format(_plan))
                    fp.close()
                    fp = open(
                        args.prefix
                        + "-plan-json-"
                        + str(count).zfill(3)
                        + "."
                        + str(worker_no),
                        mode="w",
                    )
                    fp.write("{}".format(_plan_json))
                    fp.close()

                    worker_no += 1

            cur.close()
        connection.close()

    def check(args):
        def read_queryid(prefix):
            fp = open(prefix + ".queryid", "r")
            queryid = fp.read()
            fp.close()
            return int(queryid)

        def read_planid(prefix):
            fp = open(prefix + ".planid", "r")
            queryid = fp.read()
            fp.close()
            return int(queryid)

        def read_leader_dict(prefix, no):
            path = prefix + "-plan-json-" + str(no).zfill(3) + ".0"
            js = open(path, "r")
            json_dict = json.load(js)
            js.close()
            return json_dict

        def read_worker_dicts(prefix, no):
            _dicts = []
            for i in range(1, 20):
                path = prefix + "-plan-json-" + str(no).zfill(3) + "." + str(i)
                if os.path.exists(path):
                    js = open(path, "r")
                    json_dict = json.load(js)
                    js.close()
                    _dicts.append(json_dict)
                else:
                    break
            return _dicts

        prefix = args.prefix
        no = args.no
        serverId = args.serverid
        base_dir = args.basedir

        _queryid = read_queryid(prefix)
        _planid = read_planid(prefix)
        _dict0 = read_leader_dict(prefix, no)
        _dicts = read_worker_dicts(prefix, no)

        qp = QueryProgress(base_dir, log_level=Log.debug5)
        dict_ = qp.merge_plans(_dict0, _dicts)
        _progress = qp._progress(dict_, serverId, _queryid, _planid)
        del qp

        print("progress:{}".format(_progress))

    # Parse arguments
    parser = argparse.ArgumentParser(
        description="This script is a sample how to use the pgqp module."
    )
    subparsers = parser.add_subparsers()

    # sampling command.
    parser_sampling = subparsers.add_parser("sampling", help="Sampling data")
    parser_sampling.add_argument(
        "--host",
        help='database server host or socker directory (default: "localhost")',
        default="localhost",
    )
    parser_sampling.add_argument(
        "--port", help='database server port (default: "5432")', default="5432"
    )
    parser_sampling.add_argument(
        "--dbname", help='database (default: "postgres")', default="postgres"
    )
    parser_sampling.add_argument(
        "--username", help='database user name (default: "postgres")', default="vagrant"
    )
    parser_sampling.add_argument(
        "--password", help="database user password", default=None
    )
    parser_sampling.add_argument(
        "--pid", help="default pid you want to watch", default="0"
    )
    parser_sampling.add_argument("--prefix", help="Prefix of files", default=None)
    parser_sampling.add_argument("--time", help="sampling time[sec]", default="5")

    parser_sampling.set_defaults(handler=sampling)

    # check command.
    parser_check = subparsers.add_parser("check", help="Check progress")

    parser_check.add_argument("--basedir", nargs="?", default=".")
    parser_check.add_argument("--serverid", default=None)
    parser_check.add_argument("--prefix", default=None)
    parser_check.add_argument("--no", default=None)
    parser_check.set_defaults(handler=check)

    args = parser.parse_args()
    if hasattr(args, "handler"):
        args.handler(args)
    else:
        parser.print_help()
