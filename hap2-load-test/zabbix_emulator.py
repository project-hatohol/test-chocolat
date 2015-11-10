#!/usr/bin/env python
import argparse
import logging
import sys
import cgi
import SimpleHTTPServer
import SocketServer
import json
import trigger_data
import event_data
import emulator

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
logger.setLevel(logging.INFO)

class BaseHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    __valid_tokens = set()

    def do_GET(self):
        msg = "<html></html>"
        self.wfile.write(msg)

    def do_POST(self):
        content_type, pdict = \
            cgi.parse_header(self.headers.getheader('content-type'))
        if content_type != "application/json-rpc":
            self.send_response(406)
            self.end_headers()
            logger.warning("Unknown content-type: %s" % content_type)
            return
        length = int(self.headers.getheader('content-length'))
        body = json.loads(self.rfile.read(length))

        # check JSON-RPC version
        jsonrpc = body.get("jsonrpc")
        jsonrpc = self.__get_element_with_check(body, "jsonrpc",
                                                lambda x: x == "2.0")
        if jsonrpc != "2.0":
            logger.warning("Unsupported jsonrpc version: %s" % jsonrpc)
            return

        # ID
        req_id = self.__get_element_with_check(body, "id")
        if req_id is None:
            logger.warning("Not found: req_id")
            return

        # method
        method = self.__get_element_with_check(body, "method")
        logger.debug("POST: method: %s" % method)
        if method is None:
            logger.warning("Not found: method")
            return

        # params
        params = None
        if self.__need_params(method):
            params = self.__get_element_with_check(body, "params")
            if params is None:
                logger.warning("Not found: params")
                return

        # check token
        if self.__should_check_token(method) and not self.__check_token(body):
            return

        # dispatch
        handler = self.__get_element_with_check(self.post_handlers, method)
        if handler is None:
            logger.warning("Unknwon method: %s" % method)
            return

        response = {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": handler(self, params),
        }
        self.wfile.write(json.dumps(response))

    def do_PUT(self):
        raise NotImplementedError()

    def __get_element_with_check(self, obj, name,
                                 success=lambda x: x is not None):
        p = obj.get(name)
        if not success(p):
            self.wfile.write(json.dumps(self.__invalid_json_response()))
            return None
        return p

    def __need_params(self, method):
        if method == "apiinfo.version":
            return False
        return True

    def __should_check_token(self, method):
        return not method in ("user.authenticate", "apiinfo.version")

    def __check_token(self, body):
        token = self.__get_element_with_check(body, "auth")
        if token is None:
            self.wfile.write(json.dumps(self.__invalid_json_response()))
            logger.warning("Not found: auth. token")
            return False
        if not self.validate_token(token):
            self.wfile.write(json.dumps(self.__invalid_json_response()))
            logger.warning("Invalid token: %s" % token)
            return False
        return True

    def __handler_user_authenticate(self, params):
        logger.info("Got authenticate request")
        token = self.get_token(params)
        if token is None:
            self.wfile.write(json.dumps(self.__invalid_json_response()))
            return
        return self.get_token(params)

    def __invalid_json_response(self):
        return {
            "jsonrpc":"2.0",
            "error": {
                "code":-32700,
                "message":"Parse error",
                "data":"Invalid JSON. An error occurred on the server while parsing the JSON text."
            }
        }

    def __handler_apiinfo_version(self, params):
        logger.info("Got APIInfo version")
        return "2.0.5"

    def __handler_host_get(self, params):
        msg = [{
            "maintenances": [],
            "hostid": "10084",
            "proxy_hostid": "0",
            "host": "Zabbix server",
            "status": "0",
            "disable_until": "0",
            "error": "",
            "available": "1",
            "errors_from": "0",
            "lastaccess": "0",
            "ipmi_authtype": "-1",
            "ipmi_privilege": "2",
            "ipmi_username": "",
            "ipmi_password": "",
            "ipmi_disable_until": "0",
            "ipmi_available": "0",
            "snmp_disable_until": "0",
            "snmp_available": "0",
            "maintenanceid": "0",
            "maintenance_status": "0",
            "maintenance_type": "0",
            "maintenance_from": "0",
            "ipmi_errors_from": "0",
            "snmp_errors_from": "0",
            "ipmi_error": "",
            "snmp_error": "",
            "jmx_disable_until": "0",
            "jmx_available": "0",
            "jmx_errors_from": "0",
            "jmx_error": "",
            "name": "Zabbix server",
            "flags": "0",
            "templateid": "0",
            "groups":[{"groupid": "4"}]
        },{
            "maintenances": [],
            "hostid": "10105",
            "proxy_hostid": "0",
            "host": "test1",
            "status": "0",
            "disable_until": "0",
            "error": "",
            "available": "1",
            "errors_from": "0",
            "lastaccess": "0",
            "ipmi_authtype": "0",
            "ipmi_privilege": "2",
            "ipmi_username": "",
            "ipmi_password": "",
            "ipmi_disable_until": "0",
            "ipmi_available": "0",
            "snmp_disable_until": "0",
            "snmp_available": "0",
            "maintenanceid": "0",
            "maintenance_status": "0",
            "maintenance_type": "0",
            "maintenance_from": "0",
            "ipmi_errors_from": "0",
            "snmp_errors_from": "0",
            "ipmi_error": "",
            "snmp_error": "",
            "jmx_disable_until": "0",
            "jmx_available": "0",
            "jmx_errors_from": "0",
            "jmx_error": "",
            "name": "test1",
            "flags": "0",
            "templateid": "0",
            "groups": [{"groupid": "2"}]
        }, {
            "maintenances": [],
            "hostid": "10106",
            "proxy_hostid": "0",
            "host": "test2",
            "status": "0",
            "disable_until": "0",
            "error": "",
            "available": "1",
            "errors_from": "0",
            "lastaccess": "0",
            "ipmi_authtype": "0",
            "ipmi_privilege": "2",
            "ipmi_username": "",
            "ipmi_password": "",
            "ipmi_disable_until": "0",
            "ipmi_available": "0",
            "snmp_disable_until": "0",
            "snmp_available": "0",
            "maintenanceid": "0",
            "maintenance_status": "0",
            "maintenance_type": "0",
            "maintenance_from": "0",
            "ipmi_errors_from": "0",
            "snmp_errors_from": "0",
            "ipmi_error": "",
            "snmp_error": "",
            "jmx_disable_until": "0",
            "jmx_available": "0",
            "jmx_errors_from": "0",
            "jmx_error": "",
            "name": "test2",
            "flags": "0",
            "templateid": "0",
            "groups": [{"groupid": "2"}]
        }]
        return msg


    def __handler_hostgroup_get(self, params):
        msg = [{
            "groupid": "2",
            "name": "Linux servers",
            "internal": "0",
            "flags": "0"
        }, {
            "groupid": "4",
            "name": "Zabbix servers",
            "internal": "0",
            "flags": "0"
        }]
        return msg

    def __handler_trigger_get(self, params):
        triggerids = params.get("triggerids")
        if triggerids is not None:
            triggerid_set = set(triggerids)

        result = []
        for trigger in trigger_data.extend:
            if triggerids is not None:
                if not trigger["triggerid"] in triggerid_set:
                    continue
            if params["output"] == "extend":
                result.append(trigger)
                continue

            items = {"triggerid": trigger["triggerid"]}
            for item_name in params["output"]:
                items[item_name] = trigger[item_name]
            result.append(items)
        return result

    def __handler_event_get(self, params):
        eventid_from = params.get("eventid_from")
        if eventid_from is None:
            eventid_from = 1
        eventid_till = params.get("eventid_till")
        if eventid_till is None:
            eventid_till = emulator.get_num_events(eventid_from)

        logger.debug("event.get: params: %s" % params)
        result = []
        for eventid in range(eventid_from, eventid_till+1):
            event = emulator.generate_event(eventid)
            event["eventid"] = str(eventid)
            result.append(event)
        return result

    def get_token(self, params):
        # This is too easy implementaion
        token = "0424bd59b807674191e7d77572075f33"
        self.__valid_tokens.add(token)
        return token

    def validate_token(self, token):
        return token in self.__valid_tokens

    post_handlers = {
        "user.authenticate": __handler_user_authenticate,
        "apiinfo.version": __handler_apiinfo_version,
        "host.get": __handler_host_get,
        "hostgroup.get": __handler_hostgroup_get,
        "trigger.get": __handler_trigger_get,
        "event.get": __handler_event_get,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", type=int, default=8000)
    parser.add_argument("--log-level", choices={"DEBUG", "INFO"},
                        default="INFO")
    args = parser.parse_args()

    logger.info("Logging level: %s" % args.log_level)
    exec("logger.setLevel(logging.%s)" % args.log_level)

    handler = BaseHandler
    httpd = SocketServer.TCPServer(("", args.port), handler,
                                   bind_and_activate=False)
    httpd.allow_reuse_address = True
    httpd.server_bind()
    httpd.server_activate()

    print "serving at port", args.port
    httpd.serve_forever()

if __name__ == "__main__":
    main()
