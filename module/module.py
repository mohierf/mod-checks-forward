#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
This module forwards all filtered datas to another shinken
This is useful to create "real-test traffic"
"""

from subprocess import Popen
from shinken.basemodule import BaseModule
from shinken.log import logger

properties = {
    'daemons': ['broker'],
    'type': 'checks-forward',
    'external': True,
}

def get_instance(mod_conf):
    instance = CheckForward(mod_conf)
    return instance

class CheckForward(BaseModule):
    def __init__(self, mod_conf):
        BaseModule.__init__(self, mod_conf)
        
        try:
            # Module configuration
            self.glpi_entities = getattr(mod_conf, 'glpi_entities', '')
            self.glpi_entities = self.glpi_entities.split(',')
            if len(self.glpi_entities) > 0 and self.glpi_entities[0] == '':
                self.glpi_entities = None

            self.send_nsca_bin = str(getattr(mod_conf, 'send_nsca_bin', '/usr/sbin/send_nsca'))
            self.send_nsca_config = str(getattr(mod_conf, 'send_nsca_config', '/etc/send_nsca.cfg'))

            self.nsca_server_host = str(getattr(mod_conf, 'nsca_server_host', '127.0.0.1'))
            self.nsca_server_port = int(getattr(mod_conf, 'nsca_server_port', 5667))

            logger.info("[Checks forward] module configuration, forward to: %s:%s, using %s with configuration %s", self.nsca_server_host, self.nsca_server_port, self.send_nsca_bin, self.send_nsca_config)
            if self.glpi_entities:
                logger.info("[Checks forward] module configuration, forward checks for GLPI entities: %s", str(self.glpi_entities))
            else:
                logger.info("[Checks forward] module configuration, forward checks for all hosts/services")

            # Internal cache for host entities id
            self.cache_host_entities_id = {}
        except AttributeError:
            logger.error("[Checks forward] The module is missing a property, check module configuration")
            raise
        
    def init(self):
        logger.debug("[Checks forward] init function")

    def manage_initial_host_status_brok(self, b):
        logger.debug("[Checks forward] initial host status: %s", str(b.data['customs']))
        if not self.glpi_entities:
            return
            
        self.cache_host_entities_id[b.data['host_name']] = -1
        try:
            self.cache_host_entities_id[b.data['host_name']] = b.data['customs']['_ENTITIESID']
            if self.cache_host_entities_id[b.data['host_name']] in self.glpi_entities:
                logger.info("[Checks forward] host %s checks will be forwarded (entity: %s)" % (b.data['host_name'], self.cache_host_entities_id[b.data['host_name']]))
        except:
            logger.warning("[Checks forward] no entity Id for host: %s", b.data['host_name'])
        
    def manage_host_check_result_brok(self, b):
        try:
            if self.glpi_entities and self.cache_host_entities_id[b.data['host_name']] not in self.glpi_entities:
                return
        except:
            return
            
        nsca = self.get_nsca(b)
        command = "echo \"%s\" | %s -H %s -p %s -c %s" % (nsca, self.send_nsca_bin, self.nsca_server_host, self.nsca_server_port, self.send_nsca_config)
        try:
            retcode = Popen(command, shell=True)
        except OSError as e:
            logger.error("[Checks forward] Error forward nsca '%s'" % e)
        
    def manage_service_check_result_brok(self, b):
        try:
            if self.glpi_entities and self.cache_host_entities_id[b.data['host_name']] not in self.glpi_entities:
                return
        except:
            return
            
        nsca = self.get_nsca(b)

        command = "/bin/echo \"%s\" | %s -H %s -p %s -c %s" % (nsca, self.send_nsca_bin, self.nsca_server_host, self.nsca_server_port, self.send_nsca_config)
        try:
            retcode = Popen(command, shell=True)
        except OSError as e:
            logger.error("[Checks forward] Error forward nsca '%s'" % e)

    def get_nsca(self, b):
        check_type = b.type
        hostname = b.data['host_name']
        return_code = b.data['return_code']
        output = b.data['output']

        if (check_type == "service_check_result"):
            service_description = b.data['service_description']
            # <hostname>[TAB]<service name>[TAB]<return code>[TAB]<plugin output>
            send_nsca = hostname+"\t"+service_description+"\t"+str(return_code)+"\t"+output+"|"+b.data['perf_data']
        if (check_type == "host_check_result"):
            # <hostname>[TAB]<return code>[TAB]<plugin output>
            send_nsca = hostname+"\t"+str(return_code)+"\t"+output+"|"+b.data['perf_data']

        return send_nsca

    def main(self):
        self.set_proctitle(self.name)
        self.set_exit_handler()
        while not self.interrupted:
            l = self.to_q.get()  # can block here :)
            for b in l:
                # unserialize the brok before use it
                b.prepare()
                self.manage_brok(b)
