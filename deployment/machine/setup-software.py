#!/usr/bin/env python
import pdb
import argparse
import yaml
import subprocess
import gateauchocolat
import os.path

logger = gateauchocolat.QuickLogger()

class AnsibleController(object):
    def __init__(self, args):
        self.__args = args
        self.__extra_vars = []
        self.__catalog = yaml.load(args.catalog_file)
        if args.spec_file is None:
            spec_filename = self.__catalog["spec_file"]
            logger.info("Spec. file: %s" % spec_filename)
            args.spec_file = open(spec_filename)
        self.__spec = yaml.load(args.spec_file)

        self.__check_if_using_local_db_server()

    def __add_extra_repo(self):
        repo_file = self.__args.extra_repo_file
        if repo_file is None:
            return
        self.__extra_vars.append("ext_repo_file=%s" % os.path.abspath(repo_file.name))

    def __check_if_using_local_db_server(self):
        def use_local_db(component):
            return db_server_addr == self.__find_address_of(component)

        db_server_addr = self.__find_address_of("db-server")
        self.__hatohol_server_with_local_db = use_local_db("hatohol-server")
        logger.info("Hatohol server with local DB: %s" %
                    self.__hatohol_server_with_local_db)

        self.__hatohol_web_with_local_db = use_local_db("hatohol-web")
        logger.info("Hatohol Web with local DB: %s" %
                    self.__hatohol_web_with_local_db)

    def __call__(self):
        self.__listup_machines()

    def __is_target_machine(self, machine_name):
        target_machines = self.__args.machines
        if target_machines is None:
            return True
        return machine_name in target_machines

    def __listup_machines(self):
        for machine in self.__spec["machines"]:
            machine_name = machine["name"]

            if not self.__is_target_machine(machine_name):
                continue

            ip_addrs = self.__catalog["machines"][machine_name]
            if len(ip_addrs) == 0:
                raise AssertionError("Not found: IP address: %s"  %
                                     machine_name)
            self.__setup_with_ansible(machine, ip_addrs)


    def __setup_with_ansible(self, machine_spec, ip_addrs):
        name = machine_spec["name"]
        if len(ip_addrs) > 1:
            raise AssertionError("Not supprted: multiple IP addresses: %s"
                                 % name)
        ip_addr = ip_addrs[0]
        hosts_file_path = name
        self.__create_hosts_file(hosts_file_path, ip_addr)
        for component_name in machine_spec["components"]:
            logger.info("Start setup: %s, component: %s"
                        % (name, component_name))
            self.__prepare_for_ansible_run(component_name, ip_addr)
            playbook_path = self.__get_playbook_path(component_name)
            self.__run_ansible_playbook(hosts_file_path, playbook_path)

    def __create_hosts_file(self, path, ip_addr):
        with open(path, "w") as f:
            content = "%s" % ip_addr
            f.write(content)
        logger.info("Created hosts file: %s" % path)

    def __get_playbook_path(self, name):
        return self.__args.playbook_dir + "/" + name + ".yaml"

    def __run_ansible_playbook(self, host_file, playbook):
        login_user = "centos" # TODO: read from the spec. file
        cmd = ["ansible-playbook", "-i", host_file, "-u", login_user, playbook]
        for extra_var in self.__extra_vars:
            cmd.append("-e")
            cmd.append(extra_var)
        logger.info("Command: %s" % cmd)
        proc = subprocess.Popen(cmd).communicate()
        self.__extra_vars = []

    def __prepare_for_ansible_run(self, component_name, ip_addr):
        func = {
            "db-server":      self.__prepare_for_db_server,
            "hatohol-server": self.__prepare_for_hatohol_server,
            "hatohol-web":    self.__prepare_for_hatohol_web,
        }.get(component_name)
        if func is not None:
            func(ip_addr)

    def __prepare_for_db_server(self, ip_addr):
        if self.__hatohol_server_with_local_db:
            self.__extra_vars.append("hatohol_server_with_local_db=true")
        if self.__hatohol_web_with_local_db:
            self.__extra_vars.append("hatohol_web_with_local_db=true")

    def __prepare_for_hatohol_server(self, ip_addr):
        if self.__hatohol_server_with_local_db:
            db_server_addr = "localhost"
        else:
            db_server_addr = self.__find_address_of("db-server")
        self.__generate_hatohol_conf(db_server_addr)

        self.__extra_vars.append("db_server=%s" % db_server_addr)
        self.__add_extra_repo()

    def __prepare_for_hatohol_web(self, ip_addr):
        if self.__hatohol_web_with_local_db:
            db_server_addr = "localhost"
        else:
            db_server_addr = self.__find_address_of("db-server")
        self.__extra_vars.append("db_server=%s" % db_server_addr)

        hatohol_server_addr = self.__find_address_of("hatohol-server")
        self.__extra_vars.append("hatohol_server=%s" % hatohol_server_addr)
        self.__add_extra_repo()

    # TODO: Make a reverse map for the search
    def __find_address_of(self, component):
        for machine in self.__spec["machines"]:
            if component not in machine["components"]:
                continue
            name = machine["name"]
            ip_addrs = self.__catalog["machines"][name]
            return ip_addrs[0]
        assert False

    def __generate_hatohol_conf(self, db_server_ip):
        content_lines = [
            "[mysql]",
            "database=hatohol",
            "user=hatohol",
            "password=hatohol",
            "host=%s" % db_server_ip,
            "",
            "[FaceRest]",
            "workers=4",
            "",
        ]
        content = "\n".join(content_lines)
        path = os.path.abspath(self.__args.hatohol_conf_path)
        with open(path, "w") as f:
            f.write(content)
        logger.info("Created hatohol server config: %s" % path)
        self.__extra_vars.append("hatohol_conf_path=%s" % path)

if __name__ == '__main__':
    help_spec = """
        A spec file for deploy-on-openstack.py. This option is typically used
        for machines which has been deployed without deploy-on-openstack.py
        such as physical machines.
        """
    help_catalog = \
        "A catalog file generated by deploy-on-openstack.py."
    help_machines = \
        "The setup will be done for the specified machiens with this option."
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--spec-file", type=file, help=help_spec)
    parser.add_argument("-a", "--playbook-dir", type=str, default="./",
                        help="Ansbile playbook directory")
    parser.add_argument("-m", "--machines", nargs="*", help=help_machines)
    parser.add_argument("-c", "--hatohol-conf-path", type=str,
                        default="hatohol.conf")
    parser.add_argument("-r", "--extra-repo-file", type=file)
    parser.add_argument("catalog_file", type=file, help=help_catalog)
    args = parser.parse_args()
    controller = AnsibleController(args)
    controller()
