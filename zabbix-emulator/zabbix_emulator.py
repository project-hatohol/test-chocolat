#!/usr/bin/env python
import argparse
import logging
import sys
import cgi
import SimpleHTTPServer
import SocketServer
import json

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
logger.setLevel(logging.INFO)

class BaseHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

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

        # read method
        params = self.__get_element_with_check(body, "params")
        if jsonrpc is None:
            logger.warning("Not found: params")
            return

        # method
        method = self.__get_element_with_check(body, "method")
        if method is None:
            logger.warning("Not found: method")
            return

        # dispatch
        handler = self.__get_element_with_check(self.post_handlers, method)
        if handler is None:
            logger.warning("Unknwon method: %s" % method)
            return
        self.wfile.write(json.dumps(handler(self, params, req_id)))

    def do_PUT(self):
        raise NotImplementedError()

    def __get_element_with_check(self, obj, name,
                                 success=lambda x: x is not None):
        p = obj.get(name)
        if not success(p):
            self.send_response(400)
            self.end_headers()
            return None
        return p

    def __handler_user_authenticate(self, params, req_id):
        logger.info("Got authenticate request (id: %s)" % req_id)
        msg = {
            "jsonrpc": "2.0",
            "result": "0424bd59b807674191e7d77572075f33",
            "id": req_id,
        }
        return msg

    post_handlers = {"user.authenticate": __handler_user_authenticate,}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", type=int, default=8000)
    args = parser.parse_args()

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
