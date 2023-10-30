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

mq = queue.Queue()
slot = dict()
active_ps = dict()
active = mp.Value("i", 0)


def log(*args, sep=" "):
    logging.debug(sep.join(map(str, args)))


def hello(counter: mp.Value):
    workerid = threading.get_native_id()
    with counter.get_lock():
        slot[workerid] = counter.value
        counter.value += 1
    affinity = None
    logging.debug(f"Worker: pid={os.getpid()} ID={counter.value}, TID={workerid}")
    return 0


def foo(n):
    print("foo:", n)
    time.sleep(2)
    return n


def execute(taskid, cmd, verbose=False):
    ## check in
    workerid = threading.get_native_id()
    with active.get_lock():
        active.value += 1

    bashcmd = "bash -c '%s'" % cmd
    if verbose:
        print("%d:" % taskid, bashcmd)
    p = subprocess.Popen(
        bashcmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, shell=True
    )
    with active.get_lock():
        active_ps[workerid] = p

    for line in iter(p.stdout.readline, ""):
        mq.put((workerid, taskid, line))

    p.wait()
    ## check out
    mq.put((workerid, taskid, None))
    with active.get_lock():
        active.value -= 1
        del active_ps[workerid]
    return p.returncode


def submit(executor, task_list, verbose=False):
    for task in task_list:
        executor.submit(execute, *task, verbose=verbose)


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
                        _args.append(line.rstrip())
            else:
                _args.append(x)

    cmds.append(_args)
    _args = list()

    return cmds


if __name__ == "__main__":

    def usage():
        print("USAGE: stagerun.py <OPTIONS> [ ::: <ARGUMENTS> ]* [ :::: ARGFILE ]*")
        parser_main.print_help()
        # parser_args.print_help()
        sys.exit()

    parser_main = argparse.ArgumentParser(prog="OPTIONS", add_help=False)
    parser_main.add_argument(
        "-j", "--nworkers", type=int, help="Number of workers", default=4
    )
    parser_main.add_argument("--progress", action="store_true", help="print progress")
    parser_main.add_argument("-v", "--verbose", action="store_true", help="verbose")
    parser_main.add_argument(
        "--latest-line", action="store_true", help="print only last line"
    )
    parser_main.add_argument("cmd", help="command to execute", nargs=argparse.REMAINDER)

    parser_args = argparse.ArgumentParser(prog="ARGUMENTS", add_help=False)
    parser_args.add_argument("args", help="arguments", nargs=argparse.REMAINDER)

    cmds = cmdlist(sys.argv[1:])
    if len(cmds) < 2:
        usage()

    args, _unknown = parser_main.parse_known_args(cmds[0])
    if len(_unknown) > 0:
        usage()

    args_cmd_list = list()
    for cmd in cmds[1:]:
        args_cmd, _unknown = parser_args.parse_known_args(cmd)
        if len(_unknown) > 0:
            usage()

    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")

    env = os.environ.copy()
    counter = mp.Value("i", 0)
    # pool = ProcessPoolExecutor(max_workers=args.nworkers, initializer=hello, initargs=(counter,))
    pool = ThreadPoolExecutor(
        max_workers=args.nworkers, initializer=hello, initargs=(counter,)
    )

    with pool as executor:
        args_list = cmds[1:]

        cmd = " ".join(args.cmd)
        ## check if cmd has valid formatter
        valid = any(
            a is not None or b is not None for _, a, b, _ in Formatter().parse(cmd)
        )
        if not valid:
            cmd += " {}" * len(args_list)
        log("cmd:", cmd)

        task_list = list()
        for i, argpair in enumerate(itertools.product(*args_list)):
            fullcmd = cmd.format(*argpair)
            task_list.append((i, fullcmd))

        thread = Thread(target=submit, args=(executor, task_list, args.verbose)).start()

        done = 0
        total = len(task_list)
        extra = 0
        if args.progress:
            extra += 1
        while done < total:
            try:
                workerid, taskid, line = mq.get()
                mq.task_done()

                if line is not None:
                    if args.latest_line:
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

                    if args.progress:
                        os.system("tput ll")
                        print("\r", end="", flush=True)
                        print(
                            "Processing/Done/Total/Completed(%%): %d/%d/%d/%.02f"
                            % (active.value, done, total, float(done) / total),
                            end="",
                            flush=True,
                        )
                        if not args.latest_line:
                            print("")
                        os.system("tput el")
                else:
                    done += 1
            except KeyboardInterrupt:
                log("You typed CTRL + C, which is the keyboard interrupt exception")
                for k in active_ps:
                    active_ps[k].send_signal(signal.SIGTERM)
    sys.exit(0)
