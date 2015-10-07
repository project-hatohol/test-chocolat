#!/usr/bin/env python
import pdb
import argparse
import yaml
import keystoneclient.v2_0.client as ksclient
import glanceclient
from novaclient import client as novaclient
from neutronclient.neutron import client as neutronclient
import exceptions
import urllib2
import time
import datetime
import gateauchocolat

logger = gateauchocolat.QuickLogger()

class Manager(object):
    def __init__(self, args):
        self.__args = args
        self.__spec = yaml.load(args.spec_file)
        self.__keystone = None
        self.__neutron = None
        self.__security_group_id_map = {}
        self.__deploy_catalog = {
            "machines": {},
            "spec_file": args.spec_file.name
        }

    def __call__(self):
        nova = novaclient.Client("2", **self.__get_nova_creds())
        self.__setup_security_groups()
        self.__setup_routers()
        self.__setup_networks()
        for machine in self.__spec["machines"]:
            name = machine["name"]
            self.__fixup_machine_spec(machine)
            instance = self.__get_instance_or_create(name, machine, nova)
            ip_addr = self.__show_ip_addr(instance.id)
            self.__deploy_catalog["machines"][name] = ip_addr
        self.__save_deploy_catalog()

    def __setup_security_groups(self):
        sec_grps= self.__spec.get("security_groups")
        if sec_grps is None:
            logger.info("Not found: securtiy_groups in the spec. file")
            return
        for name, sec_grp in sec_grps.items():
            secgrp = self.__create_security_group_if_needed(name, sec_grp)
            self.__security_group_id_map[name] = secgrp["id"]

    def __setup_routers(self):
        routers = self.__spec.get("routers")
        if routers is None:
            logger.info("Not found: routers in the spec. file")
            return
        for name, router in routers.items():
            self.__create_router_if_needed(name, router)

    def __setup_networks(self):
        networks = self.__spec.get("networks")
        if networks is None:
            logger.info("Not found: networks in the spec. file")
            return
        for name, network in networks.items():
            self.__create_network_if_needed(name, network)

    def __find_most_likely(self, seq, label, name):
        if len(seq) == 0:
            return None

        if len(seq) == 1:
            logger.info("Found %s: %s" % (label, name))
            return seq[0]

        ks = self.__get_keystone()
        tenant_id = ks.tenant_id
        neutron = self.get_neutron_client()
        for s in seq:
            if s["tenant_id"] == tenant_id:
                break
        else:
            raise RuntimeError(
                    "Failed to find the most likey %s from %d of %s"
                    % (len(seq), name))
        return s

    def __get_network(self, name):
        neutron = self.get_neutron_client()
        networks = neutron.list_networks(name=name)["networks"]
        network = self.__find_most_likely(networks, "network", name)
        if network is None:
            raise RuntimeError("Not round: network: %s", name)
        return network

    def __get_network_id(self, name):
        return self.__get_network(name)["id"]

    def __show_ip_addr(self, device_id, header="  "):
        neutron = self.get_neutron_client()
        do_retry = 30
        while do_retry:
            ports = neutron.list_ports(device_id=device_id)["ports"]
            if len(ports) > 0:
                break
            # it need a little time to get the address after the new
            # instance is launched. So we try to get on several occasions.
            do_retry -= 1
            time.sleep(1)
        else:
            logger.info("Not found ports for device: %s" % device_id)
            return []

        ip_addr_array = []
        for port in ports:
            for fixed_ip in port["fixed_ips"]:
                ip_addr = fixed_ip["ip_address"]
                logger.info("%s%s" % (header, ip_addr))
                ip_addr_array.append(ip_addr)
        return ip_addr_array

    def __create_security_group_if_needed(self, name, sec_grp_spec):
        neutron = self.get_neutron_client()
        kw = {"name": name}
        security_groups = neutron.list_security_groups(**kw)["security_groups"]

        sg = self.__find_most_likely(security_groups, "security group", name)
        if sg is not None:
            return sg

        params = {
            "name": name,
        }
        _sgrp = neutron.create_security_group({"security_group": params})
        security_group = _sgrp["security_group"]

        # add rules
        print security_group
        params = {
            "security_group_id": security_group["id"],
            "direction": "ingress",
        }
        neutron.create_security_group_rule({"security_group_rule": params})
        return security_group


    def __create_router_if_needed(self, name, router_spec):
        neutron = self.get_neutron_client()
        gateway_network = self.__get_network(router_spec["gateway"])
        gateway_network_id = gateway_network["id"]

        router_list = neutron.list_routers(name=name)
        for router in router_list["routers"]:
            gw = router.get("external_gateway_info")
            if gw is None:
                continue
            if gw["network_id"] == gateway_network_id:
                logger.info("Found router: %s (%s)" % (name, router["id"]))
                self.__show_ip_addr(router["id"])
                return

        # create router
        params = {
            "name": name,
        }
        router = neutron.create_router({"router": params})["router"]

        # add external gateway
        ext_gw_info = {
            "network_id": gateway_network_id,
            #"enable_snat": True,
        }
        neutron.add_gateway_router(router["id"], ext_gw_info)
        logger.info("Created router: %s (%s)" % (name, router["id"]))
        self.__show_ip_addr(router["id"])

    def __get_security_group_ids(self, machine):
        if hasattr(machine["security_group_name"], "__iter__"):
            names = machine["security_group_name"]
        else:
            names = (machine["security_group_name"],)
        return [self.__security_group_id_map[name] for name in names]


    def __get_instance_or_create(self, name, machine, nova):
        hypervisor = machine.get("hypervisor")
        if hypervisor:
            # The following line is just to make sure the hypervisor exists
            # and the returned varible: 'host' is currently not used.
            host = nova.hosts.find(host_name=hypervisor, zone="nova")
        curr_instances = nova.servers.list()
        for vm in curr_instances:
            # Is there a more efficient way to find the instance ?
            if vm.name == name:
                logger.info("Found machine: %s (%s)" % (name, vm.id))
                if hypervisor:
                    running_on = getattr(vm, "OS-EXT-SRV-ATTR:host")
                    # TODO: try to migrate if a hypervisor instead of exception
                    assert running_on == hypervisor
                return vm

        key_name = machine["key_name"]
        self.__create_keypairs_if_needed(nova, key_name)
        network_id = self.__get_network_id(machine["network_name"])
        nics = [{"net-id": network_id}]
        sv_kwargs = {
            "name": name,
            "image": self.__get_image_or_create(nova, machine),
            "flavor": self.__get_flavor_or_create(nova, machine),
            "key_name": key_name,
            "nics": nics,
            "security_groups": self.__get_security_group_ids(machine),
        }
        if hypervisor is not None:
            sv_kwargs["availability_zone"] = "nova:%s" % hypervisor
        instance = nova.servers.create(**sv_kwargs)
        logger.info("Created instance: %s (%s)" % (instance.name, instance.id))
        return instance

    def __get_creds(self, password_key="password", tenant_key="tenant_name"):
        params = (
            ("username",   "auth_username",    "OS_USERNAME"),
            (password_key, "auth_password",    "OS_PASSWORD"),
            ("auth_url",   "auth_url",         "OS_AUTH_URL"),
            (tenant_key,   "auth_tenant_name", "OS_TENANT_NAME"),
        )

        d = {}
        for key, arg_key, env_name in params:
            d[key] = self.__get_from_args_or_env(arg_key, env_name)
        return d

    def __get_nova_creds(self):
        return self.__get_creds(password_key="api_key",
                                tenant_key="project_id")

    def __get_keystone_creds(self):
        return self.__get_creds()

    def __get_keystone(self):
        if self.__keystone is not None:
            return self.__keystone
        self.__keystone = ksclient.Client(**self.__get_keystone_creds())
        return self.__keystone

    def __get_from_args_or_env(self, arg_name, env_name):
        val = self.__spec.get(arg_name)
        if val is None:
            val = os.get(env_name)
        return val

    def __fixup_machine_spec(self, machine):

        def set_if_not_existing(machine, name, value):
            if machine.get(name):
                return
            machine[name] = value

        set_if_not_existing(machine, "key_name", self.__spec["key_name"])
        set_if_not_existing(machine, "image_name", self.__spec["image_name"])
        set_if_not_existing(machine, "image_uri", self.__spec["image_uri"])
        set_if_not_existing(machine, "network_name",
                            self.__spec["network_name"])
        set_if_not_existing(machine, "security_group_name",
                            self.__spec["security_group_name"])

    def __create_keypairs_if_needed(self, nova, key_name):
        if not nova.keypairs.findall(name=key_name):
            # TODO: Implement
            raise RuntimeError("KeyPair: Not found: %s" % key_name)
            #with open(os.path.expanduser('~/.ssh/id_rsa.pub')) as fpubkey:
            #    nova.keypairs.create(name=key_name, public_key=fpubkey.read())
        logger.info("Key: %s => OK" % key_name)

    def __create_network_if_needed(self, name, network_spec):
        neutron = self.get_neutron_client()
        # Should we also check the tenant ?
        for network in neutron.list_networks()["networks"]:
            if network["name"] == name:
                network_id = network["id"]
                logger.info("Found network: %s (%s)" % (name, network_id))
                subnet = self.__create_subnets_if_needed(network_spec,
                                                         network_id)
                self.__connect_to_router_if_needed(network_spec, subnet["id"])
                break
        else:
            self.__create_network(name, network_spec)

    def __get_image_or_create(self, nova, machine):
        name = machine["image_name"]
        try:
            image = nova.images.find(name=name)
        except novaclient.exceptions.NotFound:
            image = self.__create_image(nova, machine)
        return image

    def __get_flavor_or_create(self, nova, machine):
        name = machine["flavor"]
        flavor = nova.flavors.find(name=name)
        return flavor

    def __create_image(self, nova, machine):
        ks = self.__get_keystone()
        glance_endpoint = ks.service_catalog.url_for(service_type='image',
                                                     endpoint_type='publicURL')
        glance = glanceclient.Client('1', glance_endpoint, token=ks.auth_token)
        name = machine["image_name"]
        # Check: Does urlib2.urlopen support other schema such as file:// ?
        image_uri = machine["image_uri"]
        logger.info("Start creating image: %s (%s)" % (name, image_uri))
        # We want to show the proress.
        logger.info("  This may take a while...")
        im = glance.images.create(name=name, is_public=True,
                                  disk_format="qcow2",
                                  container_format="bare",
                                  data=urllib2.urlopen(image_uri))
        logger.info("  => OK")
        return im

    def get_neutron_client(self):
        if self.__neutron is not None:
            return self.__neutron
        ks = self.__get_keystone()
        neutron_endpoint = ks.service_catalog.url_for(service_type='network',
                                                      endpoint_type='publicURL')
        neutron = neutronclient.Client("2.0", endpoint_url=neutron_endpoint,
                                       token=ks.auth_token)
        neutron.format = "json"
        self.__neutron = neutron
        return neutron

    def __create_network(self, name, network_spec):
        neutron = self.get_neutron_client()
        params = {"name": name}
        network = neutron.create_network({"network": params})["network"]

        self.__create_subnets_if_needed(network_spec, network["id"])

    def __create_subnets_if_needed(self, network_spec, network_id):
        neutron = self.get_neutron_client()
        subnets = neutron.list_subnets(network_id=network_id)

        cidr = network_spec["cidr"]
        for subnet in subnets["subnets"]:
            if subnet["cidr"] == cidr:
                logger.info("Found CIDR: %s (subnet: %s)" %
                            (cidr, subnet["id"]))
                return subnet

        params = {
            "network_id": network_id,
            "cidr": cidr,
            "ip_version": 4,
        }

        dns_server = network_spec.get("dns")
        if dns_server is not None:
            params["dns_nameservers"] = dns_server

        subnet = neutron.create_subnet({"subnet": params})
        logger.info("Created: Subnet: %s" % cidr)
        return subnet

    def __connect_to_router_if_needed(self, network_spec, subnet_id):
        neutron = self.get_neutron_client()
        router = self.__get_router(name=network_spec["router"])
        router_id = router["id"]

        # search the subnet from the attached ones
        ports = neutron.list_ports(device_id=router["id"])["ports"]
        for port in ports:
            for fixed_ip in port["fixed_ips"]:
               if fixed_ip["subnet_id"] == subnet_id:
                    logger.info("Found connected subnet: %s with router: %s" %
                                (subnet_id, router_id))
                    return

        params = {
            "subnet_id": subnet_id,
        }
        neutron.add_interface_router(router_id, params)
        logger.info("Connected interface: subnet: %s, router: %s (%s)" %
                    (subnet_id, router["name"], router_id))

    def __get_router(self, name):
        neutron = self.get_neutron_client()
        routers = neutron.list_routers(name=name)["routers"]
        router = self.__find_most_likely(routers, "router", name)
        if router is None:
            raise RuntimeError("Not round: router: %s", name)
        return router

    def __save_deploy_catalog(self):
        fmt = "deploy-catalog-%Y%m%d-%H%M%S.yaml"
        filename = datetime.datetime.now().strftime(fmt)
        with open(filename, "w") as f:
            f.write(yaml.dump(self.__deploy_catalog))
        logger.info("Saved catalog file: %s" % filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("spec_file", type=file)
    args = parser.parse_args()
    manager = Manager(args)
    manager()
