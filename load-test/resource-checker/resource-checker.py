#!/usr/bin/env python

import argparse
import logging
import subprocess
import time
import os
import sys

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
logger.setLevel(logging.INFO)

class Worker(object):

    DELIMITER = "\001"
    CHECK_INTERVAL_SEC = 5
    MEMORY_UNITS = {
        "B": 1,
        "KiB": 1024,
        "MiB": 1024*1024,
        "GiB": 1024*1024*1024,
        "TiB": 1024*1024*1024*1024,
    }

    def __init__(self, args):
        self.__args = args
        self.__prepare()
        self.__hatohol_pid = self.__get_pid("hatohol")
        self.__page_sz = 4096 # TODO: get from the target system
        self.__note("Target PID: %s" % self.__hatohol_pid)
        self.__note("Interval: %s (sec)" % self.__args.check_interval)

        self.__unit_conv_param = self.MEMORY_UNITS[self.__args.memory_unit]
        self.__note("Unit: %s (%s)" % \
                    (self.__args.memory_unit, self.__unit_conv_param))

    def __prepare(self):
        user = self.__args.user
        if user is None:
            user = os.getenv("USER")
        assert user is not None
        port = self.__args.port
        if port is None:
            port = os.getenv("PORT")
        assert port is not None

        args = ["ssh", self.__args.host, "-l", user, "-T", "-p", "%s" % port]
        self.__note(args)
        self.__ssh = subprocess.Popen(args, stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE)

    def __note(self, msg):
        logger.info("- %s" % msg)

    def __get_pid(self, target):
        pid_str, num_lines = self.__command("pgrep %s\n" % target)
        assert num_lines == 1
        return int(pid_str)

    def __command(self, cmd):
        def write_delimiter():
            self.__ssh.stdin.write("echo %s\n" % self.DELIMITER)

        is_delimiter = lambda line: line[0] == self.DELIMITER

        self.__ssh.stdin.write(cmd)
        write_delimiter()
        out = ""
        num_lines = 0
        while True:
            s = self.__ssh.stdout.readline()
            if is_delimiter(s):
                break
            out += s
            num_lines += 1
        return out, num_lines

    def __call__(self):
        while True:
            self.__read_target_memory_usage()
            time.sleep(self.__args.check_interval)

    def __read_target_memory_usage(self):
        fmt = lambda m: self.__page_sz * float(m) / self.__unit_conv_param
        line, num_lines = self.__command("cat /proc/%d/statm\n" %
                                         self.__hatohol_pid)
        assert num_lines == 1
        vm, rss, shr, txt, lib, data, dt = [fmt(m) for m in line.split()]
        logger.info("%s %s %s %s %s" % (time.time(), vm, rss, shr, data))


def main():
    parser = argparse.ArgumentParser(description="Show VM, RSS, SHR, and DATA of the target program on a remote host at regular intervals.")
    parser.add_argument("host")
    parser.add_argument("-u", "--user")
    parser.add_argument("-p", "--port", type=int, default=22)
    parser.add_argument("-c", "--check-interval", type=int, default=5)
    parser.add_argument("-m", "--memory-unit",
                        choices=Worker.MEMORY_UNITS.keys(), default="MiB")
    args = parser.parse_args()

    worker = Worker(args)
    worker()


if __name__ == "__main__":
    main()
