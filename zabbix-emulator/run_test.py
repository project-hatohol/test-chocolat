#!/usr/bin/env python
import argparse
import subprocess
import logging
import sys
import traceback
import time
import signal

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
logger.setLevel(logging.INFO)


class Manager(object):
    def __init__(self, args):
        self.__args = args
        self.__proc_zabbix_emu = None
        self.__proc_simple_sv = None

        signal.signal(signal.SIGCHLD, self.__child_handler)

    def __del__(self):
        def terminate(proc):
            if proc is None:
                return
            logger.info("Terminate: PID: %s" % proc.pid)
            proc.terminate()

        for proc in (self.__proc_zabbix_emu, self.__proc_simple_sv):
            terminate(proc)

    def __child_handler(self, signum, frame):
        logger.error("Got SIGCHLD: %s")
        assert False

    def __call__(self):
        # boot zabbix_emulator
        zabbix_emulator_args = "%s" % self.__args.zabbix_emulator_path
        kw = {
            "stdout": self.__args.zabbix_emulator_log,
            "stderr": subprocess.STDOUT,
        }
        self.__proc_zabbix_emu = subprocess.Popen(zabbix_emulator_args, **kw)
        logger.info("Launched zabbix emulator: PID: %s" % \
                    self.__proc_zabbix_emu.pid)

        # boot simple_server
        simple_server_args = "%s" % self.__args.simple_server_path
        self.__proc_simple_sv = subprocess.Popen(simple_server_args)
        logger.info("Launched simple server: PID: %s" % \
                    self.__proc_simple_sv.pid)

        time.sleep(1000) # temporary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-z", "--zabbix-emulator-path", type=str,
                        default="zabbix_emulator.py")
    parser.add_argument("-s", "--simple-server-path", type=str,
                        default="simple_server.py")
    parser.add_argument("-l", "--zabbix-emulator-log",
                        type=argparse.FileType('w'),
                        default="zabbix-emulator.log")
    args = parser.parse_args()

    manager = Manager(args)
    manager()

if __name__ == "__main__":
    try:
        main()
    except:
        logger.error("------- GOT Exception ------")
        logger.error(traceback.format_exc())
