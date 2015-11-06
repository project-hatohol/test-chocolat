#!/usr/bin/env python
import argparse
import subprocess
import logging
import sys
import traceback
import time
import signal
import re
import json

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
logger.setLevel(logging.INFO)


class Manager(object):
    def __init__(self, args):
        self.__args = args
        self.__proc_zabbix_emu = None
        self.__proc_simple_sv = None
        self.__hap2_zabbix_api = None
        self.__in_launch = False

        signal.signal(signal.SIGCHLD, self.__child_handler)

    def __subprocs(self):
        return (
            self.__proc_zabbix_emu,
            self.__proc_simple_sv,
            self.__hap2_zabbix_api,
        )

    def __del__(self):
        def terminate(proc):
            if proc is None:
                return
            logger.info("Terminate: PID: %s" % proc.pid)
            proc.terminate()

        for proc in self.__subprocs():
            terminate(proc)

    def __child_handler(self, signum, frame):
        if self.__in_launch:
            return
        logger.error("Got SIGCHLD")
        for proc in self.__subprocs():
            if proc is None:
                continue
            proc.poll()
            logger.info("PID: %s, ret.code: %s" % (proc.pid, proc.returncode))
        assert False

    def __call__(self):
        self.__launch_zabbix_emulator()
        self.__launch_simple_server()
        self.__launch_hap2_zabbix_api()

        handlers = {
            "exchangeProfile": self.__handler_exchangeProfile,
            "putEvents": self.__handler_put_event,
            "putTriggers": self.__handler_put_triggers,
            "putHosts": self.__handler_put_hosts,
            "putHostGroups": self.__handler_put_host_groups,
            "putHostGroupMembership": self.__handler_put_host_group_membership,
            "putArmInfo": self.__handler_put_arm_info,
            "getMonitoringServerInfo": self.__handler_get_ms_info,
            "getLastInfo": self.__handler_get_last_info,
        }

        while True:
            method = self.__parse_method()
            if method is None:
                continue
            handler = handlers.get(method)
            if handler is None:
                logger.warn("No handler for: %s" % method)
                continue
            handler()

    def __handler_get_ms_info(self):
        print "get_monitoring_server_info"

    def __handler_exchangeProfile(self):
        print self.__read_one_line() # body

    def __handler_get_last_info(self):
        print "get_last_info"
        self.__read_one_line() # body

    def __handler_put_event(self):
        print "put_event"
        self.__read_one_line() # summary
        self.__read_one_line() # body

    def __handler_put_triggers(self):
        print "put_triggers"
        self.__read_one_line() # body

    def __handler_put_hosts(self):
        print "put_hosts"
        self.__read_one_line() # body

    def __handler_put_host_groups(self):
        print "put_host_groups"
        self.__read_one_line() # body

    def __handler_put_host_group_membership(self):
        print "put_host_group_membership"
        self.__read_one_line() # body

    def __handler_put_arm_info(self):
        print "put_arm_info"
        self.__read_one_line()

    def __read_one_line(self):
        return self.__proc_simple_sv.stdout.readline().strip()

    def __handler_null_read(self):
        print "null read"

    def __parse_method(self):
        line = self.__proc_simple_sv.stdout.readline().rstrip()
        msg = self.__extract_message(line)
        try:
            key, method = msg.split(":", 1)
        except ValueError:
            logger.warn("Ignored unexpected line: %s" % line)
            return None
        if key != "method":
            logger.warn("Ignored unexpected message: %s" % msg)
            return None
        return method.strip()

    def __launch(self, args, kwargs):
        self.__in_launch = True
        subproc = subprocess.Popen(args, **kwargs)
        self.__in_launch = False
        if isinstance(args, list):
            progname = args[0]
        else:
            progname = args
        logger.info("Launched %s: PID: %s" % (progname, subproc.pid))
        return subproc


    def __launch_zabbix_emulator(self):
        args = "%s" % self.__args.zabbix_emulator_path
        kwargs = {
            "stdout": self.__args.zabbix_emulator_log,
            "stderr": subprocess.STDOUT,
        }
        self.__proc_zabbix_emu = self.__launch(args, kwargs)

    def __generate_ms_info_file(self):
        ms_info = {
            "serverId": 1,
            "url": "http://localhost:8000/zabbix/api_jsonrpc.php",
            "type": "8e632c14-d1f7-11e4-8350-d43d7e3146fb",
            "nickName": "HAP test server",
            "userName": "Admin",
            "password": "zabbix",
            "pollingIntervalSec": 30,
            "retryIntervalSec": 10,
            "extendedInfo": "",
        }
        f = self.__args.ms_info_file
        f.write(json.dumps(ms_info))
        f.close()

    def __launch_simple_server(self):
        self.__generate_ms_info_file()
        args = [
            "%s" % self.__args.simple_server_path,
            "--ms-info", self.__args.ms_info_file.name,
        ]
        kwargs = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.STDOUT,
        }
        self.__proc_simple_sv = self.__launch(args, kwargs)

        self.__wait_for_ready_of_simple_server()

    def __launch_hap2_zabbix_api(self):
        args = "%s" % self.__args.hap2_zabbix_api_path
        kwargs = {
            "stdout": self.__args.hap2_zabbix_api_log,
            "stderr": subprocess.STDOUT,
        }
        self.__hap2_zabbix_api = self.__launch(args, kwargs)

    def __wait_for_ready_of_simple_server(self):
        # I don't know the reason why any two characters after the number (pid)
        # is required.
        re_dispatcher = re.compile("deamonized: \d+..(Dispatcher)")
        re_receiver = re.compile("deamonized: \d+..(Receiver)")
        found_dispatcher_msg = False
        found_receiver_msg = False
        while not found_dispatcher_msg or not found_receiver_msg:
            line = self.__proc_simple_sv.stdout.readline().rstrip()
            msg = self.__extract_message(line)
            if re_dispatcher.match(msg) is not None:
                found_dispatcher_msg = True
                logger.info("Found simple_sever:dispatcher line.")
            elif re_receiver.match(msg) is not None:
                found_receiver_msg = True
                logger.info("Found simple_sever:receiver line.")

    def __extract_message(self, line):
        maxsplit = 2
        try:
            severity, component, msg = line.split(":", maxsplit)
        except:
            logger.error("Failed to split: %s" % line)
            raise
        return msg


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-z", "--zabbix-emulator-path", type=str,
                        default="zabbix_emulator.py")
    parser.add_argument("-Z", "--zabbix-emulator-log",
                        type=argparse.FileType('w'),
                        default="zabbix-emulator.log")
    parser.add_argument("-a", "--hap2-zabbix-api-path", type=str,
                        default="hap2_zabbix_api.py")
    parser.add_argument("-A", "--hap2-zabbix-api-log",
                        type=argparse.FileType('w'),
                        default="hap2-zabbix-api.log")
    parser.add_argument("-s", "--simple-server-path", type=str,
                        default="simple_server.py")
    parser.add_argument("-m", "--ms-info-file", type=argparse.FileType('w'),
                        help="MonitoringServerInfo file path that is created by this program",
                        default="ms-info.json")
    args = parser.parse_args()

    manager = Manager(args)
    manager()

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        pass
    except:
        logger.error("------- GOT Exception ------")
        logger.error(traceback.format_exc())
