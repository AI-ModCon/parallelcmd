#!/usr/bin/env python3

import os
import time
import socket
import queue
import threading
from threading import Thread

import logging
import sys
import argparse

import subprocess
import signal
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import itertools
from string import Formatter
import sqlite3
import socket
import random
import datetime

mq = queue.Queue()
slot = dict()
active_ps = dict()
active = mp.Value("i", 0)
dbfile = "pardb.sqlite"
db_retries = 10
db_retry_delay = 0.2


def log(*args, sep=" "):
    logging.debug(sep.join(map(str, args)))


def is_sqlite_lock_error(exc: Exception) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and any(
        token in str(exc).lower() for token in ("locked", "busy")
    )


def execute_sql_with_retry(con, sql, params=None, retries=None, retry_delay=None):
    max_retries = db_retries if retries is None else retries
    delay = db_retry_delay if retry_delay is None else retry_delay
    last_exc = None
    for attempt in range(max_retries):
        try:
            cur = con.cursor()
            if params is None:
                cur.execute(sql)
            else:
                cur.execute(sql, params)
            return cur
        except Exception as e:
            last_exc = e
            if is_sqlite_lock_error(e) and attempt < max_retries - 1:
                log(
                    f"SQLite locked/busy. Retry {attempt + 1}/{max_retries} in {delay:.2f}s:",
                    e,
                )
                try:
                    con.rollback()
                except Exception:
                    pass
                time.sleep(delay)
                continue
            raise
    raise last_exc


def hello(counter: mp.Value):
    workerid = threading.get_native_id()
    with counter.get_lock():
        slot[workerid] = counter.value
        counter.value += 1
    affinity = None
    logging.debug(f"Worker: pid={os.getpid()} ID={counter.value}, TID={workerid}")
    return 0


def timedelta_parse(text):
    """
    Convert input string to timedelta.
    format: [[[d-]h:]m:]s
    """
    tokens = text.replace("-", ":").split(":")
    vals = {
        key: float(val)
        for val, key in zip(tokens[::-1], ("seconds", "minutes", "hours", "days"))
    }
    return datetime.timedelta(**vals)


def execute(
    verbose=False,
    dryrun=False,
    randomorder=False,
    prefix=None,
    max_jobs=None,
    check_timeleft=None,
):
    ## check in
    hostname = socket.gethostname()
    workerid = threading.get_native_id()
    nomorejob = False
    finished = 0

    while True:
        time.sleep(random.randint(0, 10))

        if check_timeleft is not None and check_timeleft > 0:
            jobid = os.getenv("SLURM_JOB_ID", None)
            assert jobid is not None, "SLURM_JOB_ID not found in environment variables."
            cmd = f"squeue -h -j {jobid} -o %L"
            proc = subprocess.run(cmd.split(), stdout=subprocess.PIPE)
            timestr = proc.stdout.decode("utf-8").strip()
            try:
                left = timedelta_parse(timestr).total_seconds()
            except Exception as e:
                ## "INVALID" when remaining time is less than a few seconds
                log(f"{slot[workerid]}: Exception parsing time string '{timestr}':", e)
                left = 0.0
            log(
                f"{slot[workerid]}: SLURM job {jobid} remaining time: {left:.2f} seconds"
            )

            if left < check_timeleft:
                log(
                    f"{slot[workerid]}: Remaining time {left:.2f} seconds is less than threshold {check_timeleft}. Stop fetching new jobs."
                )
                break

        with sqlite3.connect(dbfile) as con:
            while True:
                try:
                    con.execute("BEGIN EXCLUSIVE;")
                    cur = con.cursor()
                    if randomorder:
                        sql = f"SELECT Seq, Command FROM parjob WHERE Exitval is NULL ORDER BY RANDOM() LIMIT 1;"
                    else:
                        sql = f"SELECT Seq, Command FROM parjob WHERE Exitval is NULL LIMIT 1;"
                    cur.execute(sql)
                    row = cur.fetchone()
                    if not row:
                        log(f"{slot[workerid]}: No more job")
                        nomorejob = True
                        break

                    (
                        taskid,
                        cmd,
                    ) = row
                    cur.execute(
                        f"UPDATE parjob SET Starttime = unixepoch('now'), Exitval = -1000 WHERE Seq = {taskid};"
                    )
                    log(f"{slot[workerid]}: taskid, cmd:", taskid, cmd)
                    assert cur.rowcount == 1
                    con.commit()
                    break
                except Exception as e:
                    log(f"{slot[workerid]}: Exception:", e)
                    pass

        if nomorejob:
            break

        bashcmd = "bash -c '%s'" % cmd
        if prefix is not None:
            bashcmd = "%s bash -c '%s'" % (prefix, cmd)
        if verbose:
            print("%d: cmd:" % taskid, bashcmd)

        if not dryrun:
            starttime = time.time()
            with active.get_lock():
                active.value += 1

            p = subprocess.Popen(
                bashcmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                shell=True,
                env=os.environ.copy(),
            )
            with active.get_lock():
                active_ps[workerid] = p

            with sqlite3.connect(dbfile) as con:
                execute_sql_with_retry(
                    con,
                    f"UPDATE parjob SET Hostname = '{hostname}', PID = {p.pid} WHERE Seq = {taskid};",
                )
                con.commit()

            try:
                for line in iter(p.stdout.readline, ""):
                    mq.put((workerid, taskid, line))
            except Exception as e:
                log(f"{slot[workerid]}: Exception:", e)
                print(line.rstrip(), flush=True)

            p.wait()
            ## check out
            mq.put((workerid, taskid, None))
            with active.get_lock():
                active.value -= 1
                del active_ps[workerid]

            runtime = time.time() - starttime
            if verbose:
                print("%d: Done:" % taskid, p.returncode)

            with sqlite3.connect(dbfile) as con:
                execute_sql_with_retry(
                    con,
                    f"UPDATE parjob SET Exitval = {p.returncode}, JobRuntime = {runtime} WHERE Seq = {taskid};",
                )
                con.commit()
            finished += 1

            if max_jobs is not None and finished >= max_jobs:
                break
    return 0


def jobcount():
    def dojob():
        with sqlite3.connect(dbfile) as con:
            cur = con.cursor()
            cur.execute(
                "SELECT count(1), sum(case when Exitval >= 0 then 1 else 0 end) FROM parjob;"
            )
            row = cur.fetchone()
            (
                total,
                done,
            ) = row
        return (total, done)

    while True:
        try:
            return dojob()
        except Exception as e:
            log("Exception:", e)
            log("Sleep and try again ...")
            time.sleep(1)


def cmdlist(argv):
    """
    return list of list
    """
    cmds = list()
    _args = list()
    _type = 0  ## 0: regular, 1: file
    for x in argv:
        if x == ":::":
            cmds.append(_args)
            _args = list()
            _type = 0
        elif x == "::::":
            cmds.append(_args)
            _args = list()
            _type = 1
        else:
            if _type == 1:
                with open(x, "r") as f:
                    for line in f.readlines():
                        if line.startswith("#") or line.strip() == "":
                            continue
                        _args.append(line.rstrip())
            else:
                _args.append(x)

    cmds.append(_args)

    return cmds


def progress(done, total, dashboard=False, progress=False, timeskip=0.0):
    def putline():
        os.system("tput ll")
        print("\r", end="", flush=True)
        print(
            "Processing/Done/Total/Completed(%%)/Time(sec): %d/%d/%d/%.01f%%/%.02fs"
            % (
                active.value,
                done,
                total,
                float(done) / total * 100,
                time.time() - t0,
            ),
            end="",
            flush=True,
        )
        if not dashboard:
            print("")
        os.system("tput el")

    extra = 1 if progress else 0
    t0 = time.time()
    t1 = time.time()
    t2 = time.time()
    while True:
        workerid, taskid, line = mq.get()
        if (workerid is None) or (done == total):
            total, done = jobcount()
            putline()
            break

        if line is not None:
            if time.time() - t2 > timeskip:
                t2 = time.time()
            else:
                continue

            if dashboard:
                os.system("tput ll")
                print("\r", end="", flush=True)
                os.system("tput sc")
                for i in range(slot[workerid] + extra):
                    os.system("tput cuu1")
                print("%d:" % taskid, line.rstrip(), end="", flush=True)
                os.system("tput el")
                os.system("tput rc")
            else:
                print("%d:" % taskid, line, end="", flush=True)

            if progress:
                ## try not too frequent
                if time.time() - t1 > 2:
                    total, done = jobcount()
                    putline()
                    t1 = time.time()
                else:
                    pass


def print_table(cur, rows, row_format):
    colnames = [desc[0] for desc in cur.description]
    bars = ["-" * len(desc[0]) for desc in cur.description]
    print(row_format.format(*colnames))
    print(row_format.format(*bars))
    for row in rows:
        print(
            row_format.format(
                *map(
                    lambda x: str(x) if not isinstance(x, float) else "%.2f" % x,
                    row,
                )
            )
        )


def selectdb(cur, filter=None):
    if filter is None:
        filter = "1=1"

    cur.execute(
        "SELECT Seq, "
        "datetime(Starttime, 'unixepoch', 'localtime') as Starttime, "
        "Hostname, PID, JobRuntime, Exitval, Command "
        "FROM parjob "
        f"WHERE {filter};"
    )
    rows = cur.fetchall()
    row_format = " {:>4} {:<19} {:<15} {:>8} {:>11} {:>7} {:<80}"
    print_table(cur, rows, row_format)
    return rows


def checkdb(args):
    with sqlite3.connect(dbfile) as con:
        filter = "1=1"
        if args.where is not None:
            filter = f"{args.where}"
        if args.like is not None:
            filter = f"Command LIKE '{args.like}'"
        if args.id:
            if not isinstance(args.id, list):
                args.id = [args.id]
            filter = "Seq IN (%s)" % ",".join(map(str, args.id))

        # con.row_factory = sqlite3.Row
        cur = con.cursor()
        # cur.execute("SELECT count(1) as Total, sum(case when Exitval >= 0 then 1 else 0 end) as Finished FROM parjob")
        if args.list:
            selectdb(cur, filter)
        else:
            cur.execute(
                "SELECT count(1) as Total, "
                "sum(case when Exitval == -1000 then 1 else 0 end) as Processing, "
                "sum(case when Exitval >= 0 then 1 else 0 end) as Finished, "
                "sum(case when Exitval > 0 then 1 else 0 end) as 'Nonzero Exit' "
                "FROM parjob;"
            )
            row = cur.fetchone()
            row_format = " {:>5} {:>10} {:>8} {:>12}"
            print_table(cur, [row], row_format)


def resetdb(args):
    with sqlite3.connect(dbfile) as con:
        filter = "Exitval <> 0"
        if args.where is not None:
            filter = f"{args.where}"
        if args.like is not None:
            filter = f"Command LIKE '{args.like}'"
        if args.all:
            filter = "1=1"
        if args.id:
            if not isinstance(args.id, list):
                args.id = [args.id]
            filter = "Seq IN (%s)" % ",".join(map(str, args.id))

        cur = con.cursor()
        rows = selectdb(cur, filter)
        # (count,) = cur.fetchone()
        count = len(rows)
        ans = input("%d number of rows will be reset. Continue? (Y/N): " % count)
        if ans == "Y" or ans == "y":
            cur = execute_sql_with_retry(
                con,
                f"UPDATE parjob SET Starttime = NULL, Hostname = NULL, PID = NULL, JobRuntime = NULL, Exitval = NULL WHERE {filter};",
            )
            print("Reset:", cur.rowcount)
            con.commit()
        else:
            print("Aborted.")


def deletedb(args):
    with sqlite3.connect(dbfile) as con:
        filter = "Exitval <> 0"
        if args.like is not None:
            filter = f"Command LIKE '{args.like}'"
        if args.all:
            filter = "1=1"
        if args.id:
            if not isinstance(args.id, list):
                args.id = [args.id]
            filter = "Seq IN (%s)" % ",".join(map(str, args.id))

        cur = con.cursor()
        rows = selectdb(cur, filter)
        count = len(rows)
        ans = input("%d number of rows will be deleted. Continue? (Y/N): " % count)
        if ans == "Y" or ans == "y":
            cur = execute_sql_with_retry(con, f"DELETE FROM parjob WHERE {filter};")
            print("Delete:", cur.rowcount)
            con.commit()
        else:
            print("Aborted.")


def updatedb(args):
    with sqlite3.connect(dbfile) as con:
        filter = "1 = 1"
        if args.like is not None:
            filter = f"Command LIKE '{args.like}'"
        if args.id:
            if not isinstance(args.id, list):
                args.id = [args.id]
            filter = "Seq IN (%s)" % ",".join(map(str, args.id))

        cur = con.cursor()
        rows = selectdb(cur, filter)
        replace_a, replace_b = args.replace.split(",")
        for row in rows:
            cmd = row[-1]
            new_cmd = cmd.replace(replace_a, replace_b)

            print(
                row_format.format(
                    *map(
                        lambda x: str(x) if not isinstance(x, float) else "%.2f" % x,
                        row,
                    )
                ),
                "->",
                new_cmd,
            )

        count = len(rows)
        ans = input("%d number of rows will be updated. Continue? (Y/N): " % count)
        if ans == "Y" or ans == "y":
            affected_rowcount = 0
            for row in rows:
                seq = row[0]
                cmd = row[-1]
                new_cmd = cmd.replace(replace_a, replace_b)

                cur = execute_sql_with_retry(
                    con, "UPDATE parjob SET Command = ? WHERE Seq = ?;", (new_cmd, seq)
                )
                affected_rowcount += cur.rowcount

            print("Updated:", affected_rowcount)
            con.commit()
        else:
            print("Aborted.")


def initdb(args):
    cmds = cmdlist(sys.argv[1:])
    args_list = cmds[1:]
    cmd = " ".join(args.cmd)
    ## check if cmd has valid formatter
    valid = any(a is not None or b is not None for _, a, b, _ in Formatter().parse(cmd))
    if not valid:
        cmd += " {}" * len(args_list)

    task_list = list()
    for i, argpair in enumerate(itertools.product(*args_list)):
        fullcmd = cmd.format(*argpair)
        task_list.append((i, fullcmd))

    if args.reverse:
        task_list.reverse()

    with sqlite3.connect(dbfile) as con:
        cur = con.cursor()
        if not args.append:
            try:
                sql = "DROP TABLE parjob;"
                cur.execute(sql)
            except:
                pass

            # Enable WAL mode
            cur.execute("PRAGMA journal_mode=WAL;")
            sql = (
                "CREATE TABLE IF NOT EXISTS parjob ("
                "  Seq INTEGER PRIMARY KEY AUTOINCREMENT,"
                "  Starttime FLOAT(44),"
                "  Hostname TEXT,"
                "  PID INT,"
                "  JobRuntime FLOAT(44),"
                "  Exitval BIGINT,"
                "  Command TEXT);"
            )
            cur.execute(sql)
            print("%s created" % (dbfile))

        inserted_rows = 0
        for i, cmd in task_list:
            sql = "SELECT 1 FROM parjob WHERE Command = '%s';" % (cmd,)
            cur.execute(sql)
            exists = cur.fetchone()
            if args.check_dup and exists:
                print("Already exists. Skip:", cmd)
            else:
                sql = "INSERT INTO parjob (Command) VALUES ('%s');" % (cmd,)
                cur = execute_sql_with_retry(con, sql)
                inserted_rows += cur.rowcount
        con.commit()
        print("%d tasks added." % (inserted_rows))
        # res = cur.execute("select count(*) from parjob;")
        # (ntotal,) = res.fetchone()
        # print("%d Total added." % (ntotal))


def exec(args):
    total, done = jobcount()

    if args.dashboard:
        ## print empty lines to leave space for each worker
        for i in range(args.nworkers + 1):
            print("")

    p = threading.Thread(
        target=progress,
        args=(
            done,
            total,
            args.dashboard,
            args.progress,
            args.timeskip,
        ),
    )
    p.start()

    env = os.environ.copy()
    counter = mp.Value("i", 0)
    # pool = ProcessPoolExecutor(max_workers=args.nworkers, initializer=hello, initargs=(counter,))
    pool = ThreadPoolExecutor(
        max_workers=args.nworkers, initializer=hello, initargs=(counter,)
    )

    with pool as executor:
        future_list = list()
        for index in range(args.nworkers):
            future = executor.submit(
                execute,
                verbose=args.verbose,
                dryrun=args.dryrun,
                randomorder=args.randomorder,
                prefix=args.prefix,
                max_jobs=args.max_jobs,
                check_timeleft=args.check_timeleft,
            )
            future_list.append(future)

        for future in future_list:
            future.result()

        mq.put((None, None, None))
        p.join()


def run(args):
    initdb(args)
    exec(args)


def add_default_run_subcommand(argv, subcommand_names):
    if any(x in ("-h", "--help") for x in argv):
        return argv

    if any(x in subcommand_names for x in argv):
        return argv

    idx = 0
    while idx < len(argv):
        token = argv[idx]
        if token in ("--dbfile", "--log_level"):
            idx += 2
            continue
        if token.startswith("--dbfile=") or token.startswith("--log_level="):
            idx += 1
            continue
        break

    out = list(argv)
    out.insert(idx, "run")
    return out


if __name__ == "__main__":

    def usage():
        # print(
        #     "USAGE: %s <OPTIONS> [ ::: <ARGUMENTS> ]* [ :::: ARGFILE ]*" % (sys.argv[0])
        # )
        parser_main.print_help()
        # parser_args.print_help()
        sys.exit()

    parser_main = argparse.ArgumentParser(prog="OPTIONS")
    parser_main.add_argument("--dbfile", help="dbfile", default="pardb.sqlite")
    parser_main.add_argument(
        "--db_retries",
        type=int,
        default=10,
        help="Max retries when SQLite database is locked",
    )
    parser_main.add_argument(
        "--log_level",
        choices=["debug", "info"],
        default="info",
        help="Set log level (debug or info)",
    )

    subparsers = parser_main.add_subparsers(
        title="subcommands", description="valid subcommands", dest="command"
    )

    ## subcommand: check
    parser = subparsers.add_parser("check")
    parser.add_argument("-l", "--list", action="store_true", help="list")
    parser.add_argument("--where", help="where statement")
    parser.add_argument("--like", help="like statement")
    parser.add_argument("--id", type=int, help="select by id", nargs="+")
    parser.set_defaults(func=checkdb)

    ## subcommand: reset
    parser = subparsers.add_parser("reset")
    parser.add_argument("--where", help="where statement")
    parser.add_argument("--like", help="like statement")
    parser.add_argument("-a", "--all", action="store_true", help="reset all")
    parser.add_argument("--id", type=int, help="reset by id", nargs="+")
    parser.set_defaults(func=resetdb)

    ## subcommand: delete
    parser = subparsers.add_parser("delete")
    parser.add_argument("--like", help="like statement")
    parser.add_argument("-a", "--all", action="store_true", help="delete all")
    parser.add_argument("--id", type=int, help="remove by id", nargs="+")
    parser.set_defaults(func=deletedb)

    ## subcommand: init
    parser = subparsers.add_parser("init")
    parser.set_defaults(func=initdb)
    parser.add_argument("cmd", help="command to execute", nargs=argparse.REMAINDER)
    parser.add_argument("-v", "--verbose", action="store_true", help="verbose")
    parser.add_argument("-a", "--append", action="store_true", help="append")
    parser.add_argument("-r", "--reverse", action="store_true", help="reverse")
    parser.add_argument(
        "--check_dup", action="store_true", help="allow duplicate commands"
    )

    ## subcommand: exec
    parser = subparsers.add_parser("exec")
    parser.set_defaults(func=exec)
    parser.add_argument(
        "-j", "--nworkers", type=int, help="Number of workers", default=4
    )
    parser.add_argument("--progress", action="store_true", help="print progress")
    parser.add_argument("--dashboard", action="store_true", help="print only last line")
    parser.add_argument("--dryrun", action="store_true", help="dryrun")
    parser.add_argument("-v", "--verbose", action="store_true", help="verbose")
    parser.add_argument("--timeskip", type=float, help="timeskip", default=0.0)
    parser.add_argument("--randomorder", action="store_true", help="randomorder")
    parser.add_argument("--prefix", help="command prefix")
    parser.add_argument(
        "--max_jobs", type=int, help="maximum number of jobs per process to run"
    )
    parser.add_argument("--check_timeleft", type=float, help="check timeleft (seconds)")

    ## subcommand: run (init + exec)
    parser = subparsers.add_parser("run")
    parser.set_defaults(func=run)
    parser.add_argument("cmd", help="command to execute", nargs=argparse.REMAINDER)
    parser.add_argument("-v", "--verbose", action="store_true", help="verbose")
    parser.add_argument("-a", "--append", action="store_true", help="append")
    parser.add_argument("-r", "--reverse", action="store_true", help="reverse")
    parser.add_argument(
        "--check_dup", action="store_true", help="allow duplicate commands"
    )
    parser.add_argument(
        "-j", "--nworkers", type=int, help="Number of workers", default=4
    )
    parser.add_argument("--progress", action="store_true", help="print progress")
    parser.add_argument("--dashboard", action="store_true", help="print only last line")
    parser.add_argument("--dryrun", action="store_true", help="dryrun")
    parser.add_argument("--timeskip", type=float, help="timeskip", default=0.0)
    parser.add_argument("--randomorder", action="store_true", help="randomorder")
    parser.add_argument("--prefix", help="command prefix")
    parser.add_argument(
        "--max_jobs", type=int, help="maximum number of jobs per process to run"
    )
    parser.add_argument("--check_timeleft", type=float, help="check timeleft (seconds)")

    ## subcommand: update
    parser = subparsers.add_parser("update")
    parser.add_argument("--replace", help="replace statement in the form of 'old,new'")
    parser.add_argument("--like", help="like statement")
    parser.add_argument("--id", type=int, help="reset by id", nargs="+")
    parser.set_defaults(func=updatedb)

    raw_argv = add_default_run_subcommand(sys.argv[1:], set(subparsers.choices.keys()))
    cmds = cmdlist(raw_argv)
    args, _unknown = parser_main.parse_known_args(cmds[0])
    if len(_unknown) > 0:
        idx_list = [i for i, x in enumerate(cmds[0]) if x in subparsers.choices.keys()]
        idx = min(idx_list) if len(idx_list) > 0 else 0

        ## re-arrange and try again
        for x in reversed(_unknown):
            cmds[0].remove(x)
            cmds[0].insert(idx, x)

        args, _unknown = parser_main.parse_known_args(cmds[0])
        if len(_unknown) > 0:
            print("Unknown options:", _unknown)
            usage()

    level = logging.DEBUG if args.log_level == "debug" else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")

    log("Python version:", ".".join(map(str, sys.version_info[:3])))
    log("Python info:", sys.version)

    dbfile = args.dbfile
    db_retries = max(1, args.db_retries)

    if args.command is None:
        usage()
    args.func(args)
    sys.exit(0)
