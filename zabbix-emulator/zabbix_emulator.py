#!/usr/bin/env python
import argparse
import logging
import sys
import cgi
import SimpleHTTPServer
import SocketServer

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
        if content_type != "application/json":
            self.send_response(406)
            self.end_headers()
            return
        length = int(self.headers.getheader('content-length'))
        msg = "<html>POST</html>"
        self.wfile.write(msg)

    def do_PUT(self):
        raise NotImplementedError()

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
