#!/usr/bin/env python
import argparse
import logging
import libchocoload
import yaml
from hatohol import hatohol_def

logger = logging.getLogger(__name__)

class Worker(object):

    def __init__(self, args, id_number, parameters):
        self.__args = args
        self.__paramters = parameters
        self.__serial = -libchocoload.NUM_SELF_TRIGGER_EVENT

        pattern = parameters["pattern"]
        self.__sv_param = parameters["servers"][id_number]
        self.__generator = libchocoload.PATTERNS[pattern]["single"]

        self.__arg = {
          "base_name": "events-generator",
          "id_number": id_number,
        }

    def parseEvent(self, event):
        def is_self_trigger_event():
            return self.__serial <= 0

        self.__serial += 1
        self.__arg["serial"] = self.__serial
        base_event = self.__generator(**self.__arg)
        if is_self_trigger_event():
            self.__verifyEventSelfTriggerEvent(base_event, event)
        else:
            self.__verifyLoadedEvent(base_event, event)

    def checkProcessedCount(self):
        self.__assert(self.__serial, self.__sv_param["num_events"])
        logger.info("[%d] Checked %d events." %
                    (self.__sv_param["id"], self.__serial))

    def __assert(self, value, option=None, comparator=lambda v, e: v == e,
                 error_formatter=None):
        if comparator(value, option):
            return

        if error_formatter:
            msg = error_formatter(value, option)
        else:
            msg = "value: %s, option: %s" % (value, option)
        raise AssertionError(msg)

    def __assert_not_none(self, value):
        self.__assert(value, comparator=lambda v, o: v is not None)

    def __assert_gt(self, value, option):
        self.__assert(value, option, comparator=lambda v, o: v > o)

    def __assert_ge(self, value, option):
        self.__assert(value, option, comparator=lambda v, o: v >= o)

    def __verifyEventCommon(self, base, actual):
        self.__assert(actual["serverId"], self.__sv_param["id"])

        # We should make sure 'unifiedId' is not duplicated
        self.__assert_gt(actual["unifiedId"], 0)

        # We should make sure the consistency of the time
        self.__assert_not_none(actual.get("time"))

    def __verifyEventSelfTriggerEvent(self, base, actual):
        self.__verifyEventCommon(base, actual)
        self.__assert(actual["type"], hatohol_def.EVENT_TYPE_GOOD)
        self.__assert(actual["status"], hatohol_def.TRIGGER_STATUS_OK)
        self.__assert(actual["hostId"], "__SELF_MONITOR")
        self.__assert_not_none(actual.get("triggerId"))
        self.__assert(actual["severity"],
                      hatohol_def.TRIGGER_SEVERITY_EMERGENCY)
        self.__assert(actual["eventId"], "")
        self.__assert_ge(len(actual["brief"]), 1)
        self.__assert(actual["extendedInfo"], "")

    def __verifyLoadedEvent(self, base, actual):
        self.__verifyEventCommon(base, actual)
        self.__assert(actual["type"], libchocoload.TYPE_MAP[base["type"]])
        self.__assert(actual["status"], libchocoload.STATUS_MAP[base["status"]])
        self.__assert(actual["hostId"], base["hostId"])
        self.__assert(actual["triggerId"], base["triggerId"])
        self.__assert(actual["severity"],
                      libchocoload.SEVERITY_MAP[base["severity"]])
        self.__assert(actual["eventId"], base["eventId"])
        self.__assert(actual["brief"], base["brief"])
        self.__assert(actual["extendedInfo"], base["extendedInfo"])


class Manager(object):
    MAX_EVENTS_CHUNK = 1000

    def __init__(self, args):
        self.__args = args
        self.__parameters = yaml.load(args.parameter_file)
        args.parameter_file.close()
        logger.info("Loaded paramters: %s", args.parameter_file.name)

        self.__hatohol_rest = libchocoload.HatoholRestApi(args)

        self.__workers = {}
        for idx, server_info in enumerate(self.__parameters["servers"]):
            sv_id = server_info["id"]
            self.__workers[sv_id] = Worker(args, idx, self.__parameters)
            logger.info("Created worker for serverId: %d" % sv_id)

    def __call__(self):
        def get_max_count_msg():
            if self.__args.max_num_events == libchocoload.NUM_EVENTS_UNLIMITED:
                return "Unlimited"
            return "%d" % self.__args.max_num_events

        logger.info("Chunk size: %d, Max count: %s" %
            (self.MAX_EVENTS_CHUNK, get_max_count_msg()))

        self.__hatohol_rest.login()
        offset = 0
        count = 0

        def should_continue():
            if self.__is_num_events_unlimited():
                return True
            num_max_count = \
                self.__args.max_num_events + self.NUM_SELF_TRIGGER_EVENT
            return count < num_max_count

        while should_continue():
            params = {
                "sortType": "unifiedId",
                "sortOrder": hatohol_def.DATA_QUERY_OPTION_SORT_ASCENDING,
                "limit": self.MAX_EVENTS_CHUNK,
                "offset": offset,
            }
            response = self.__hatohol_rest.request("/events", params)
            events = response["events"]
            num_events = len(events)
            if num_events == 0:
                break
            count += num_events
            self.__parseEvents(response["events"])
            offset += self.MAX_EVENTS_CHUNK
        logger.info("Checked %d events" % count)

        for worker in self.__workers.values():
            worker.checkProcessedCount()

    def __is_num_events_unlimited(self):
        return self.__args.max_num_events == libchocoload.NUM_EVENTS_UNLIMITED

    def __parseEvents(self, events):
        for event in events:
            sv_id = event["serverId"]
            self.__workers[sv_id].parseEvent(event)


if __name__ == "__main__":
    libchocoload.setup_logger(logger)
    parser = argparse.ArgumentParser()

    libchocoload.HatoholRestApi.define_arguments(parser)
    parser.add_argument("-m", "--max-num-events", type=int,
                        default=libchocoload.NUM_EVENTS_UNLIMITED,
                        help="The number of events to be generated. 0 means unlimited.")
    parser.add_argument("-f", "--parameter-file", type=file,
                        default=libchocoload.DEFAULT_PARAMETER_FILE)

    args = parser.parse_args()
    mgr = Manager(args)
    try:
        mgr()
    except KeyboardInterrupt:
        logger.info("Exit: KeyboardInterrupt")
