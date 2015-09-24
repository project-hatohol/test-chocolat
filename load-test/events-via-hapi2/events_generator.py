#!/usr/bin/env python
import argparse
import logging
import json
import pika
import multiprocessing
import datetime
import sys
import traceback
import libchocoload
import yaml
import time
from hatohol import hatohol_def

logger = logging.getLogger(__name__)


def run_with_keyboard_interrupt_exit(task, *task_args, **task_kwargs):
    try:
        task(*task_args, **task_kwargs)
    except KeyboardInterrupt:
        pass

class Counter(object):
    def __init__(self, show_interval=1):
        self.__count = 0
        self.__prev_show_time = None
        self.__show_interval = show_interval
        self.__prev_serial = 0

    def show_info(self, serial):
        if self.__prev_show_time is None:
            self.__prev_show_time = datetime.datetime.utcnow()
            self.__prev_serial = serial
            return

        now = datetime.datetime.utcnow()
        time_diff = (now - self.__prev_show_time).total_seconds()
        if time_diff < self.__show_interval:
            return
        serial_diff = serial - self.__prev_serial
        logger.info("%.3f events/sec.", serial_diff / time_diff)
        self.__prev_show_time = now
        self.__prev_serial = serial

class AMQPBaseWorker(object):
    def __init__(self, args):
        self.__args = args
        self.__request_id = 1

        host, port, virt_host = self.__parse_url(args.amqp_broker_url)
        credentials = pika.credentials.PlainCredentials(args.amqp_user,
                                                        args.amqp_password)
        conn_args = {
            "host": host,
            "port": port,
            "virtual_host": virt_host,
            "credentials": credentials,
        }

        # Should we pass "frame_max" ?

        param = pika.connection.ConnectionParameters(**conn_args)
        self.__connection = \
            pika.adapters.blocking_connection.BlockingConnection(param)
        self.__channel = self.__connection.channel()
        self.__channel.queue_declare(queue=args.queue_name)

    def get_name(self):
        return "%s:%s" % \
               (self.__args.hap_base_name, self.__args.id_number)

    def __parse_url(self, url):
        schema = "amqp"
        host =""
        port = None
        target = url

        # schema
        idx = target.find("://")
        if idx != -1:
            schema = target[:idx]
            target = target[idx+3:]

        # host and vhost
        idx = target.find("/")
        if idx != -1:
            host = target[:idx]
            target = target[idx+1:]
        else:
            host = target
            target = "/"

        # extract port
        idx = host.find(":")
        if idx != -1:
            host = host[:idx]
            port = host[idx+1:]

        # vhost
        vhost = target

        return host, port, vhost

    def get_channel(self):
        return self.__channel

    def get_queue_name(self):
        return self.__args.queue_name

    def request(self, method, params):
        msg = {
            "id": self.__request_id,
            "params": params,
            "method": method,
            "jsonrpc": "2.0",
        }
        self.publish(msg)
        self.__request_id += 1

    def publish(self, msg):
        self.__channel.basic_publish(
            exchange="", routing_key=self.__args.queue_name + "-S",
            body=json.dumps(msg),
            properties=pika.BasicProperties(content_type="application/json"))

class HapiWorker(AMQPBaseWorker):
    def __init__(self, args):
        AMQPBaseWorker.__init__(self, args)

    def exchange_profile(self, msg_id=None):
        params = {
            "name": "test event generator",
            "procedures": [
                "exchangeProfile",
                "updateMonitoringServerInfo",
            ],
        }

        if msg_id is None:
            self.request("exchangeProfile", params)
        else:
            msg = {
                "id": msg_id,
                "result": params,
                "jsonrpc": 2
            }
            self.publish(msg)


class Generator(HapiWorker):

    def __init__(self, args):
        HapiWorker.__init__(self, args)
        self.__args = args
        self.__counter = Counter()

        def num_events_msg():
            msg = "# of events: "
            num_events = self.__args.num_events
            if num_events == libchocoload.NUM_EVENTS_UNLIMITED:
                msg += "Unlimited"
            else:
                msg += "%d" % num_events
            return msg

        logger.info("Generator: %s, %s" % (self.get_name(), num_events_msg()))

    def __call__(self):
        run_with_keyboard_interrupt_exit(self.__main_loop)

    def __main_loop(self):
        self.exchange_profile()
        i = 1
        chunk_size = self.__args.chunk_size
        logger.info("Chunk size: %d" % chunk_size)

        def should_continue():
            num_events = self.__args.num_events
            if num_events == libchocoload.NUM_EVENTS_UNLIMITED:
                return True
            return i <= num_events

        generator = libchocoload.PATTERNS[self.__args.pattern]["batch"]
        while should_continue():
            params = generator(i, chunk_size,
                               self.__args.hap_base_name,
                               self.__args.id_number)
            self.request("putEvents", params)
            i += chunk_size
            self.__counter.show_info(i)
        logger.info("Completed: Generator: %s" % self.get_name())



class Receiver(HapiWorker):
    def __init__(self, args):
        HapiWorker.__init__(self, args)
        self.__args = args
        logger.info("Receiver: %s" % self.get_name())

        num_events = self.__args.num_events
        self.__unlimited_mode = \
            num_events == libchocoload.NUM_EVENTS_UNLIMITED
        self.__calculate_last_response_id()

    def __calculate_last_response_id(self):
        NUM_PREPROC_RESPONSE = 1

        num_events = self.__args.num_events
        chunk_sz = self.__args.chunk_size
        self.__expect_last_response_id = \
            (num_events + (chunk_sz - 1)) / chunk_sz
        self.__expect_last_response_id += NUM_PREPROC_RESPONSE

    def __call__(self):
        run_with_keyboard_interrupt_exit(self.__main_loop)

    def __main_loop(self):
        channel = self.get_channel()
        channel.basic_consume(self.__consume_handler_wrapper,
                              queue=self.get_queue_name() + "-T",
                              no_ack=True)
        channel.start_consuming()

    def __consume_handler_wrapper(self, *args, **kwargs):
        """
        When several kinds of exceptions such as AttributeError happen in
        __consume_handler(), they are caught in pika library. The library,
        then, raises Connection.Closed(). This structure make us difficult to
        see the line and the cause of the exception. So this method catches
        exceptions here once and show them.
        """
        try:
            self.__consume_handler(*args, **kwargs)
        except:
            traceback.print_exception(*(sys.exc_info()))
            raise

    def __consume_handler(self, ch, method, properties, body):
        response = json.loads(body)
        method = response.get("method")
        if method is not None:
            self.__handle_method(method, response)
            return
        if self.__looks_like_exchange_profile(response):
            logger.info("Got response of exchange profile")
            return

        if response["result"] != "SUCCESS":
            logger.error("Bad response: %s" % body)
            if not self.__args.ignore_result_failure:
                raise RuntimeError()

        if self.__is_last_response(response):
            ch.stop_consuming()

    def __is_last_response(self, response):
        if self.__unlimited_mode:
            return False;
        return response["id"] == self.__expect_last_response_id

    def __handle_method(self, method, msg):
        if method == "exchangeProfile":
            self.exchange_profile(msg["id"])
        else:
            logger.error("Got method: %s" % method)
            raise RuntimeError()

    def __looks_like_exchange_profile(self, response):
        result = response.get("result")
        if result is None:
            return False
        if isinstance(result, unicode):
            return False
        if result.get("name") is None:
            return False
        procedures = result.get("procedures")
        if procedures is None:
            return False
        if not isinstance(procedures, list):
            return False
        return True

class Manager(object):

    def __init__(self, args):
        self.__args = args
        self.__generators = []
        self.__parameters = {
            "pattern": args.pattern,
            "servers": [],
        }
        self.__hatohol_rest = libchocoload.HatoholRestApi(args)

    def __call__(self):
        self.__hatohol_rest.login()
        self.__delete_existing_monitoring_servers()
        self.__register_event_generators_as_monitoring_servers()
        self.main_loop()

    def __delete_existing_monitoring_servers(self):
        response = self.__hatohol_rest.request("/server")
        for sv in response["servers"]:
            logger.info("Delete existing monitoring server: %s (ID: %s)" %
                        (sv["nickname"], sv["id"]))
            url = "/server/%s" % sv["id"]
            self.__hatohol_rest.request(url, method="DELETE")

    def __register_event_generators_as_monitoring_servers(self):
        def register(index):
            data = {
                "type": hatohol_def.MONITORING_SYSTEM_HAPI2,
                # work as default Zabbix plugin
                "uuid": "8e632c14-d1f7-11e4-8350-d43d7e3146fb",
                "nickname": "%s:%s" % (self.__args.hap_base_name, index),
                "hostName": "",
                "ipAddress": "",
                "port": 0,
                "pollingInterval": 1,
                "retryInterval": 1,
                "userName": "",
                "password": "",
                "baseURL": "",
                "extendedInfo": "",
                "passiveMode": False,
                "brokerUrl": self.__args.amqp_broker_url,
                "staticQueueAddress": self.__get_amqp_queue_address(index),
            }
            res = self.__hatohol_rest.request("/server", data, method="POST")
            msg = "Registered a monitoring server for event generation: " \
                  "%s (ID: %d)" % (data["nickname"], res["id"])
            logger.info(msg)
            server_info = {"id": res["id"]}
            self.__parameters["servers"].append(server_info)

        for i in range(self.__args.num_generators):
            register(i)

    def main_loop(self):
        self.__start_time = time.time()
        num_events_list = libchocoload.distribute_number(
                            self.__args.num_total_events,
                            self.__args.num_generators)
        for id_number in range(self.__args.num_generators):
            _args = {
                "queue_name": self.__get_amqp_queue_address(id_number),
                "id_number": id_number,
                "num_events": num_events_list[id_number],
            }
            for key in dir(self.__args):
                if key[0] != "_":
                    _args[key] = getattr(self.__args, key)
            args = type("GeneratorArg", (object,), _args)()

            if not self.__args.dont_generate:
                gen = multiprocessing.Process(target=Generator(args))
                gen.start()
                self.__generators.append(gen)
            else:
                logger.info("Skip: Generater: %s:%s" %
                            (self.__args.hap_base_name, id_number))

            rcv = multiprocessing.Process(target=Receiver(args))
            rcv.start()
            # __generators should be renamed
            self.__generators.append(rcv)

            server_info = self.__parameters["servers"][id_number]
            server_info["num_events"] = args.num_events
        self.__join_all()

    def __join_all(self):
        idx = 0
        TIMEOUT = 0.5 # second
        while len(self.__generators) > 0:
            proc = self.__generators[idx]
            pid = proc.pid
            proc.join(TIMEOUT)
            if not proc.is_alive():
                del self.__generators[idx]
                logger.info("Joined one process: %d" % pid)

            idx += 1
            if idx >= len(self.__generators):
                idx = 0


    def get_elapsed_time(self):
        return time.time() - self.__start_time

    def save_paramter_file(self):
        with open(self.__args.parameter_file, "w") as f:
            f.write(yaml.dump(self.__parameters))
        logger.info("Saved parameters: %s", self.__args.parameter_file)

    def __get_amqp_queue_address(self, index):
        return "%s.%s" % (self.__args.hap_base_name, index)

    def __get_amqp_args(self, index):
        args = {
            "amqp_broker_url": self.__args.amqp_broker_url,
            "queue_name": self.__get_amqp_queue_address(index),
            "user_name": self.__args.amqp_user,
            "password": self.__args.amqp_password,
        }
        return args


def main():
    libchocoload.setup_logger(logger)

    parser = argparse.ArgumentParser()
    libchocoload.HatoholRestApi.define_arguments(parser)
    parser.add_argument("amqp_broker_url", help="A ULR of the AMQP broker")
    parser.add_argument("-g", "--num-generators", default=1, type=int,
                        help="The number of event generators working via HAPI2.0")
    parser.add_argument("--hap-base-name", default="events-generator")
    parser.add_argument("--amqp-user", default="guest")
    parser.add_argument("--amqp-password", default="guest")
    parser.add_argument("-s", "--dont-generate", action="store_true",
                        help="Don't generate events althought receiver works.")
    parser.add_argument("-m", "--only-main-loop", action="store_true",
                        help="Run generation loop only.")
    parser.add_argument("-r", "--ignore-result-failure", action="store_true",
                        help="Continue if the result is FALURE.")
    parser.add_argument("-c", "--chunk-size", type=int, default=1,
                        help="The number of events in one putEvents call.")
    parser.add_argument("-n", "--num-total-events", type=int,
                        default=libchocoload.NUM_EVENTS_UNLIMITED,
                        help="The number of events to be generated. 0 means unlimited.")
    parser.add_argument("-p", "--pattern", type=str,
                        default="simple", choices=libchocoload.PATTERNS.keys())
    parser.add_argument("-f", "--parameter-file", type=str,
                        default=libchocoload.DEFAULT_PARAMETER_FILE)

    args = parser.parse_args()
    mgr = Manager(args)
    try:
        if args.only_main_loop:
            logger.info("Run main loop only.")
            mgr.main_loop()
        else:
            mgr()
    except KeyboardInterrupt:
        logger.info("Exit: KeyboardInterrupt")

    elapsed_time = mgr.get_elapsed_time()
    logger.info("Elapsed time: %.3f [s], rate: %.1f [events/s]" %
                (elapsed_time, float(args.num_total_events)/elapsed_time))
    mgr.save_paramter_file()


if __name__ == "__main__":
    main()
