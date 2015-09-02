#!/usr/bin/env python
import urllib
import urllib2
import logging
import json
import datetime
from hatohol import hatohol_def

logger = logging.getLogger(__name__)

NUM_EVENTS_UNLIMITED = 0

def setup_logger(_logger):
    handler = logging.StreamHandler()
    _logger.addHandler(handler)
    _logger.setLevel(logging.INFO)


def get_current_hapi_utc_time():
    now = datetime.datetime.utcnow()
    return now.strftime("%Y%m%d%H%M%S.%f")


def generate_event_std_elem(serial, base_name, id_number):
    elem = {
        "extendedInfo": "sampel extended info",
        "brief": "example brief",
        "eventId": "%d" % serial,
        "time": get_current_hapi_utc_time(),
        "type": "GOOD",
        "triggerId": "2",
        "status": "OK",
        "severity": "INFO",
        "hostId": "3",
        "hostName": "exampleName"
    }
    return elem

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
