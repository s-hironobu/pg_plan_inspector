#!/usr/bin/env python3
"""

This is a repository management tool for the plan_analyzer module.

Usage:
 repo_mgr.py create [--basedir XXX]
 repo_mgr.py get    [--basedir XXX] serverid
 repo_mgr.py push   [--basedir XXX] serverid
 repo_mgr.py show   [--basedir XXX] [--verbose]
 repo_mgr.py check  [--basedir XXX]
 repo_mgr.py rename [--basedir XXX] old_serverid new_serverid
 repo_mgr.py delete [--basedir XXX] serverid
 repo_mgr.py reset  [--basedir XXX] serverid
 repo_mgr.py recalc [--basedir XXX] serverid

  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 2021-2024, Hironobu Suzuki @ interdb.jp
"""

import argparse
import sys
from pgpi import Common, Repository, GetTables, Grouping, Regression, Log, PushParam

if __name__ == "__main__":

    #LOG_LEVEL = Log.info
    LOG_LEVEL = Log.debug3
    REPOSITORY = Common.REPOSITORY_DIR

    msg_basedir = "Base directory of repository (Default: '.')"
    msg_serverid = "Server identifier"

    # Functions
    def repository_create(args):
        base_dir = args.basedir
        rp = Repository(base_dir, log_level=LOG_LEVEL)
        rp.create_repo()
        print("Created {}".format(base_dir + "/" + REPOSITORY + "."))
        del rp

    def get_data(args):
        base_dir = args.basedir
        serverId = args.serverid
        print("Use {}:".format(base_dir + "/" + REPOSITORY))
        gt = GetTables(base_dir, log_level=LOG_LEVEL)
        _num_rows = gt.get_tables(serverId)
        if _num_rows > 0:
            gp = Grouping(base_dir, log_level=LOG_LEVEL)
            gp.grouping(serverId)
            rg = Regression(base_dir, log_level=LOG_LEVEL)
            rg.regression(serverId)
            del gp, rg
        del gt

    def push_data(args):
        base_dir = args.basedir
        serverId = args.serverid
        print("Use {}:".format(base_dir + "/" + REPOSITORY))
        pp = PushParam(base_dir, log_level=LOG_LEVEL)
        pp.push_param(serverId)
        del pp

    def check_data(args):
        base_dir = args.basedir
        print("Use {}:".format(base_dir + "/" + REPOSITORY))
        rp = Repository(base_dir, log_level=LOG_LEVEL)
        rp.check_host_conf_file()
        rp.check_dirs()
        del rp

    def rename_data(args):
        base_dir = args.basedir
        old_serverId = args.old_serverid
        new_serverId = args.new_serverid
        print("Use {}:".format(base_dir + "/" + REPOSITORY))
        rp = Repository(base_dir, log_level=LOG_LEVEL)
        rp.rename_serverId(old_serverId, new_serverId)
        del rp

    def delete_data(args):
        base_dir = args.basedir
        serverId = args.serverid
        print("Use {}:".format(base_dir + "/" + REPOSITORY))
        rp = Repository(base_dir, log_level=LOG_LEVEL)
        if rp.check_serverId(serverId) == False:
            del rp
            print(
                "Error: Directory:{} does not exist in {}".format(
                    serverId, base_dir + "/" + REPOSITORY
                )
            )
            sys.exit(1)
        while True:
            _msg = "Delete " + serverId + "'s data? (yes, no)"
            try:
                _input = input(_msg)
                if str.lower(_input) == "yes":
                    if rp.remove_serverId(serverId):
                        print(
                            "Deleted directory:{} in {}".format(
                                serverId, base_dir + "/" + REPOSITORY
                            )
                        )
                    else:
                        print(
                            "Could not delete {} directory in {}".format(
                                serverId, base_dir + "/" + REPOSITORY
                            )
                        )
                    break
                elif str.lower(_input) == "no":
                    break
            except KeyboardInterrupt:
                del rp
                print("\nInput Interrupted.\n")
                sys.exit(1)
        del rp

    def show_data(args):
        base_dir = args.basedir
        print("Use {}:".format(base_dir + "/" + REPOSITORY))
        rp = Repository(base_dir, log_level=LOG_LEVEL)
        rp.show_hosts(args.verbose)
        del rp

    def reset_data(args):
        base_dir = args.basedir
        serverId = args.serverid
        print("Use {}:".format(base_dir + "/" + REPOSITORY))
        rp = Repository(base_dir, log_level=LOG_LEVEL)
        print("Reset grouping")
        rp.reset_grouping_dir(serverId)
        print("Reset regression")
        rp.reset_regression_dir(serverId)
        del rp

    def recalc_data(args):
        base_dir = args.basedir
        serverId = args.serverid
        print("Use {}:".format(base_dir + "/" + REPOSITORY))
        gp = Grouping(base_dir, log_level=LOG_LEVEL)
        gp.grouping(serverId)
        rg = Regression(base_dir, log_level=LOG_LEVEL)
        rg.regression(serverId)
        del gp, rg

    # Create command parser.
    parser = argparse.ArgumentParser(
        description="This is a repository management tool for the plan_analyze module."
    )
    subparsers = parser.add_subparsers()

    # create command.
    parser_create = subparsers.add_parser("create", help="Create an empty repository")
    parser_create.add_argument("--basedir", nargs="?", default=".", help=msg_basedir)
    parser_create.set_defaults(handler=repository_create)

    # get command.
    parser_get = subparsers.add_parser(
        "get",
        help="Get the rows from the query_plan.log table of the specified server",
    )
    parser_get.add_argument("--basedir", nargs="?", default=".", help=msg_basedir)
    parser_get.add_argument("serverid", help=msg_serverid)
    parser_get.set_defaults(handler=get_data)

    # push command.
    parser_push = subparsers.add_parser(
        "push",
        help="Push the regression params to the specified server",
    )
    parser_push.add_argument("--basedir", nargs="?", default=".", help=msg_basedir)
    parser_push.add_argument("serverid", help=msg_serverid)
    parser_push.set_defaults(handler=push_data)

    # show command.
    parser_show = subparsers.add_parser(
        "show", help="Show server info in the hosts.conf"
    )
    parser_show.add_argument("--basedir", nargs="?", default=".", help=msg_basedir)
    parser_show.add_argument("--verbose", action="store_true", help="Show details")
    parser_show.set_defaults(handler=show_data)

    # check command.
    parser_check = subparsers.add_parser(
        "check",
        help="Check the security of the repository and the validation of the server-ids in the hosts.conf",
    )
    parser_check.add_argument("--basedir", nargs="?", default=".", help=msg_basedir)
    parser_check.set_defaults(handler=check_data)

    # rename command.
    parser_rename = subparsers.add_parser(
        "rename", help="Rename old_serverid to new_serverid."
    )
    parser_rename.add_argument("--basedir", nargs="?", default=".", help=msg_basedir)
    parser_rename.add_argument("old_serverid", help="Old server id")
    parser_rename.add_argument("new_serverid", help="New server id")
    parser_rename.set_defaults(handler=rename_data)

    # delete command.
    parser_delete = subparsers.add_parser(
        "delete", help="Delete the data of the specified server-id in the repository"
    )
    parser_delete.add_argument("--basedir", nargs="?", default=".", help=msg_basedir)
    parser_delete.add_argument("serverid", help=msg_serverid)
    parser_delete.set_defaults(handler=delete_data)

    # reset command.
    parser_reset = subparsers.add_parser(
        "reset",
        help="Delete only the grouping and regression data of the specified server-id in the repository",
    )
    parser_reset.add_argument("--basedir", nargs="?", default=".", help=msg_basedir)
    parser_reset.add_argument("serverid", help=msg_serverid)
    parser_reset.set_defaults(handler=reset_data)

    # recalc command.
    parser_recalc = subparsers.add_parser(
        "recalc",
        help="Recalculate the grouping and regression data of the specified server-id in the repository",
    )
    parser_recalc.add_argument("--basedir", nargs="?", default=".", help=msg_basedir)
    parser_recalc.add_argument("serverid", help=msg_serverid)
    parser_recalc.set_defaults(handler=recalc_data)

    # Main procedure.
    args = parser.parse_args()
    if hasattr(args, "handler"):
        args.handler(args)
    else:
        parser.print_help()
