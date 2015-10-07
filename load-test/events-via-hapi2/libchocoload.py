#!/usr/bin/env python
import urllib
import urllib2
import logging
import json
import datetime
from hatohol import hatohol_def

logger = logging.getLogger(__name__)

NUM_EVENTS_UNLIMITED = 0
NUM_SELF_TRIGGER_EVENT = 2

DEFAULT_PARAMETER_FILE = "parameters.yaml"

TYPE_MAP = {
    "GOOD": hatohol_def.EVENT_TYPE_GOOD,
    "BAD": hatohol_def.EVENT_TYPE_BAD,
    "NOTIFICATION": hatohol_def.EVENT_TYPE_NOTIFICATION,
}

STATUS_MAP = {
    "OK": hatohol_def.TRIGGER_STATUS_OK,
    "NG": hatohol_def.TRIGGER_STATUS_PROBLEM,
}

SEVERITY_MAP = {
    "UNKNOWN": hatohol_def.TRIGGER_SEVERITY_UNKNOWN,
    "INFO": hatohol_def.TRIGGER_SEVERITY_INFO,
    "WARNING": hatohol_def.TRIGGER_SEVERITY_WARNING,
    "ERROR": hatohol_def.TRIGGER_SEVERITY_ERROR,
    "CRITICAL": hatohol_def.TRIGGER_SEVERITY_CRITICAL,
    "EMERGENCY": hatohol_def.TRIGGER_SEVERITY_EMERGENCY,
}

def setup_logger(_logger):
    handler = logging.StreamHandler()
    _logger.addHandler(handler)
    _logger.setLevel(logging.INFO)
    fmt = "%(asctime)s %(levelname)8s [%(process)5d] %(name)s:%(lineno)d:  " \
          "%(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    _logger.addHandler(handler)


def get_current_hapi_utc_time():
    now = datetime.datetime.utcnow()
    return now.strftime("%Y%m%d%H%M%S.%f")


def generate_event_simple_elem(serial, base_name, id_number):

    host_id = serial % 100
    host_name = "exampleHost-%s-%s-%s" % (base_name, id_number, host_id)

    brief = "base_name: %s, id_number: %s, serial: %s" % \
            (base_name, id_number, serial)

    def get_item_in_sequence(items, serial):
        return items.keys()[serial % len(items)]

    event_type = get_item_in_sequence(TYPE_MAP, serial)
    status = get_item_in_sequence(STATUS_MAP, serial)
    severity = get_item_in_sequence(SEVERITY_MAP, serial)

    extended_info = "{\"serial\": %s}" % serial
    elem = {
        "extendedInfo": "{sampel extended info",
        "brief": brief,
        "eventId": "e:%010d" % serial,
        "time": get_current_hapi_utc_time(),
        "type": event_type,
        "triggerId": "t%04d" % (serial % 10000),
        "status": status,
        "severity": severity,
        "hostId": "h%02d" % host_id,
        "hostName": host_name,
    }
    return elem


def generate_event_simple(first_serial, num_events, base_name, id_number):
    params = {
        # "fetchId": "1",
        # "mayMoreFlag": True,
        "lastInfo": "last:%20d" % (first_serial + num_events - 1),
        "events": []
    }
    for i in range(num_events):
        elem = generate_event_simple_elem(first_serial + i,
                                          base_name, id_number)
        params["events"].append(elem)
    return params


PATTERNS = {
  "simple": {"single": generate_event_simple_elem,
             "batch":  generate_event_simple},
}


def distribute_number(total_number, num_div):
    if total_number == NUM_EVENTS_UNLIMITED:
        return [0 for i in range(num_div)]

    base = total_number / num_div
    mod = total_number % num_div
    ret = [base + 1 for i in range(mod)]
    ret.extend([base for i in range(num_div-mod)])
    return ret

class HatoholRestApi(object):

    DEFAULT_HATOHOL_PORT = 33194

    @classmethod
    def define_arguments(cls, parser):
        parser.add_argument("hatohol_server",
                            help="A host name or IP address of the target Hatohol server")
        parser.add_argument("--hatohol-user", default="admin")
        parser.add_argument("--hatohol-password", default="hatohol")

    def __init__(self, args):
        self.__args = args
        self.__hatohol_session_id = None
        self.__create_hatohol_url(args)

    def __create_hatohol_url(self, args):
        url = args.hatohol_server
        if url.find(":") == -1:
            url += ":%d" % self.DEFAULT_HATOHOL_PORT
        self.__hatohol_url = "http://" + url
        logger.info("Hatohol URL: %s" % self.__hatohol_url)

    def login(self):
        logger.info("Try to login: hathol server: %s" %
                    self.__args.hatohol_server)
        query = {"user": self.__args.hatohol_user,
                 "password": self.__args.hatohol_password}
        response = self.request("/login", query)
        self.__hatohol_session_id = response["sessionId"]
        logger.info("Succeeded in login: API version: %d" %
                    response["apiVersion"])

    def request(self, path, data=None, method="GET"):
        url = self.__hatohol_url + path
        if method == "GET" and data is not None:
            url += "?" + urllib.urlencode(data)
            data = None
        if data is not None:
            data = urllib.urlencode(data)
        req = urllib2.Request(url, data=data)
        req.get_method = lambda: method
        if self.__hatohol_session_id is not None:
            req.add_header(hatohol_def.FACE_REST_SESSION_ID_HEADER_NAME,
                           self.__hatohol_session_id)
        response = json.load(urllib2.urlopen(req))
        if response["errorCode"] != hatohol_def.HTERR_OK:
            msg = "Failed to REST request: code: %d" % response["errorCode"]
            err_msg = response.get("errorMessage")
            if err_msg is not None:
                msg += ", " + err_msg
            raise RuntimeError(msg)
        return response
