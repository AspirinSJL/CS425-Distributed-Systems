import socket
import yaml
import gossiper
import xmlrpclib
import logging

LOGGER = logging.getLogger('Nimbus')

def find_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 0))
    return s.getsockname()[0]

class Nimbus(object):
    def __init__(self):
        super(Nimbus, self).__init__()

        # nimbus need to know the global membership to manage the cluster
        self.gossiper = gossiper.Gossiper(self)
        self.gossiper.run()

        # Configuration for the system
        with open('crane_conf.yaml') as f:
            self.crane_conf = yaml.load(f)

        self.ip = find_local_ip()

        # ip -> (ip, alive, used, id)
        ## focus on physical
        self.all_machines = {}

        # Include current machine assignment
        ## focus on logical
        self.topo = None

    def add_machine(self, ip):
        if ip in self.all_machines and self.all_machines[ip]['alive']:
            return

        self.all_machines[ip] = {
            'ip': ip,
            'alive': True,
            'used': False,
        }

    def remove_machine(self, ip):
        # Skip already detected and handled during children's failure handling
        if self.all_machines[ip]['alive']:
            # Mark it as dead
            self.all_machines[ip]['alive'] = False
            if self.all_machines[ip]['used']:
                self.handle_failure(ip)

    def reserve_alive_machines(self):
        for machine_id, machine_conf in self.topo['machines'].iteritems():
            ip = self.assign_alive_machine(machine_id)
            assert ip != None
            machine_conf['ip'] = ip

    def assign_alive_machine(self, id):
        for m in self.all_machines.values():
            if m['alive'] and not m['used']:
                m['used'] = True
                m['id'] = id
                return m['ip']
            else:
                continue
        return None

    def handle_failure(self, ip):
        machine_status = self.all_machines[ip]
        assert not machine_status['alive'] and machine_status['used']

        failed_machine_id = machine_status['id']

        # Find an alive machine
        substitute_ip = self.assign_alive_machine(failed_machine_id)
        print "alive substitute", substitute_ip

        # Retrieve the according conf to update and replay
        machine_conf = self.topo['machines'][failed_machine_id]

        # Update local topo
        # 1. for the failed machine itself
        machine_conf['ip'] = substitute_ip
        # 2. for the parents of the nodes
        for node_id, node_conf in machine_conf['nodes'].iteritems():
            # Redirect parent's downstream ip
            if 'parent' in node_conf:
                parent_id = node_conf['parent']
                parent_machine_id =int(parent_id.split('-')[0])

                # Should finish before RPC, in case parent is also failed and need to be recursively restarted
                self.topo['machines'][parent_machine_id]['nodes'][parent_id]['children'][node_id]['ip'] = substitute_ip

        # Sync topo updates to physical machines
        supervisor_port = self.crane_conf['supervisor']['rpc_port']
        substitute = xmlrpclib.ServerProxy('http://%s:%d' % (substitute_ip, supervisor_port), allow_none=True)
        for node_id, node_conf in machine_conf['nodes'].iteritems():
            # Assume children ip are valid
            # Start the node
            substitute.start_node(node_conf)

            # Redirect parent's downstream ip
            if 'parent' in node_conf:
                parent_id = node_conf['parent']
                parent_machine_id = int(parent_id.split('-')[0])

                # Will start a new parent anyway, the new parent knows the latest ip
                if parent_machine_id == failed_machine_id:
                    continue

                # Locate parent's machine to sync the conf
                parent_ip = self.topo['machines'][parent_machine_id]['ip']
                print "parent_ip", parent_ip
                try:
                    print "start parent update"
                    # Parent is alive, update is enough
                    parent_machine = xmlrpclib.ServerProxy('http://%s:%d' % (parent_ip, supervisor_port), allow_none=True)
                    parent_machine.update_child_ip(parent_id, node_id, substitute_ip)
                    print "end parent update"
                except Exception as e:
                    print e
                    print 'parent also fails, restart it'
                    # Parent is also failed, restart it totally
                    self.remove_machine(parent_ip)

    def start_app(self, name, topo_conf):
        LOGGER.info('Starting app')
        with open(topo_conf) as f:
            self.topo = yaml.load(f)

        if len(self.topo['machines']) > len(filter(lambda v: v['alive'] and not v['used'], self.all_machines.values())):
            print 'No enough alive machines'

        # The machine id assignment
        self.reserve_alive_machines()

        # Set up the machines except spout's
        supervisor_port = self.crane_conf['supervisor']['rpc_port']
        for machine_id, machine_conf in self.topo['machines'].iteritems():
            # The first machine is used for a single spout, which should be last started to avoid tuple loss
            if machine_id == 0:
                continue

            print 'staring on machine: ', machine_conf['ip'], supervisor_port

            supervisor = xmlrpclib.ServerProxy('http://%s:%d' % (machine_conf['ip'], supervisor_port), allow_none=True)
            for node_id, node_conf in machine_conf['nodes'].iteritems():
                # Complete the spout info dynamically
                spout_id = node_conf['spout']['id']
                spout_machine_id = int(spout_id.split('-')[0])

                node_conf['spout']['ip'] = self.topo['machines'][spout_machine_id]['ip']
                # node_conf['spout']['port'] = self.topo['machines'][spout_machine_id]['nodes'][spout_id]['ack_port']

                # Complete the children info dynamically if needed
                if 'children' in node_conf:
                    for child_id, child_conf in node_conf['children'].iteritems():
                        child_machine_id = int(child_id.split('-')[0])

                        child_conf['ip'] = self.topo['machines'][child_machine_id]['ip']
                        child_conf['tuple_port'] = self.topo['machines'][child_machine_id]['nodes'][child_id]['tuple_port']

                # Start the node (may wait for the spout for tuples feed)
                print "starting node", node_conf
                supervisor.start_node(node_conf)

        # TODO: refactor
        # Start spout
        root_machine_conf = self.topo['machines'][0]
        supervisor = xmlrpclib.ServerProxy('http://%s:%d' % (root_machine_conf['ip'], supervisor_port), allow_none=True)
        # Ensure spout is the last one to start
        for node_id, node_conf in reversed(root_machine_conf['nodes'].items()):
            # Complete the children info dynamically if needed
            if 'children' in node_conf:
                for child_id, child_conf in node_conf['children'].iteritems():
                    child_machine_id = int(child_id.split('-')[0])

                    child_conf['ip'] = self.topo['machines'][child_machine_id]['ip']
                    child_conf['tuple_port'] = self.topo['machines'][child_machine_id]['nodes'][child_id]['tuple_port']

            # Start the spout
            supervisor.start_node(node_conf)

    def run(self):
        promote = '''
            run [app] [topo conf]\n
            stop [app]\n
        '''
        while True:
            cmd = raw_input(promote).strip().split()
            if cmd[0] == 'run':
                # self.start_app('test', 'topo_conf.yaml')
                self.start_app(cmd[1], cmd[2])
            elif cmd[0] == 'stop':
                pass

if __name__ == '__main__':
    nimbus = Nimbus()

    nimbus.run()