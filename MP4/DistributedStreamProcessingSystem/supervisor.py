import socket
import json
import csv
import time
import random
import thread
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import operator
import yaml
import gossiper
# import logging
#
# LOGGER = logging.getLogger('Supervisor')

def find_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 0))
    return s.getsockname()[0]

class Supervisor(object):
    def __init__(self, crane_conf):
        super(Supervisor, self).__init__()

        self.crane_conf = crane_conf

        # supervisor needn't know global membership, but just contribute some info
        self.gossiper = gossiper.Gossiper()
        self.gossiper.run()

        self.nodes = {}

    def start_node(self, node_conf):
        print "starting node", node_conf
        if node_conf['class'] == 'spout':
            node = Spout(node_conf, self.crane_conf)
        elif node_conf['class'] == 'bolt':
            node = Bolt(node_conf, self.crane_conf)
        else:
            raise ValueError

        self.nodes[node_conf['id']] = node
        thread.start_new_thread(node.run, ())

    def update_child_ip(self, node_id, child_id, new_ip):
        node = self.nodes[node_id]
        # For later UDP, the new ip will be used
        node.node_conf['children'][child_id]['ip'] = new_ip

    def ack(self, spout_id, message_id, partial_xor):
        self.nodes[spout_id].ack(message_id, partial_xor)

class Node(object):
    def __init__(self, node_conf, crane_conf):
        super(Node, self).__init__()
        
        self.node_conf = node_conf
        self.crane_conf = crane_conf
        
        self.ip = find_local_ip()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send_to_children(self, tuple):
        for child_conf in self.node_conf['children'].values():
            try:
                self.sock.sendto(json.dumps(tuple), (child_conf['ip'], child_conf['tuple_port']))
            except Exception as e:
                print e

    def run(self):
        pass

class Spout(Node):
    def __init__(self, node_conf, crane_conf):
        super(Spout, self).__init__(node_conf, crane_conf)

        self.acked_message = 0
        # message_id -> (xor, timestamp, tuple)
        self.buffer = {}
        self.message_counter = 0
        self.timeout = 5

    def run(self):
        with open(self.node_conf['input'], 'rb') as input:
            reader = csv.reader(input)
            for i, row in enumerate(reader):
                # Resend timeout messages before sending a new one
                if i % 10 == 0:
                    self.check_timeout()
                # Wait for acks
                while len(self.buffer) >= self.node_conf['buffer_size']:
                    time.sleep(1)
                    self.check_timeout()

                tuple = {
                    # Metadata
                    'message_id': self.message_counter,
                    'tuple_id': random.randint(1, (1 << 8) - 1),

                    'content': row,
                }
                self.message_counter += 1

                # Emit tuple to each child
                time.sleep(0.5)
                self.send_to_children(tuple)

                # Local record
                self.buffer[tuple['message_id']] = {
                    'xor': tuple['tuple_id'] * (len(self.node_conf['children']) % 2),
                    'timestamp': time.time(),
                    'tuple': tuple,
                }

                print "buf: %d, msg_cnt: %d" % (len(self.buffer), self.message_counter)

        # Wait for acks
        while self.buffer:
            time.sleep(1)
            self.check_timeout()

        print 'acked_message: %d before finishing' % self.acked_message


    def ack(self, message_id, partial_xor):
        print "acking message_id: %d, coming xor: %d" % (message_id, partial_xor)

        # Premature timeout happened before
        # No need to care about the stale acks
        if message_id not in self.buffer:
            print 'stale msg, nvm'
            return

        print "self xor", self.buffer[message_id]['xor']
        self.buffer[message_id]['xor'] ^= partial_xor
        self.buffer[message_id]['timestamp'] = time.time()
        # Finished
        if self.buffer[message_id]['xor'] == 0:
            del self.buffer[message_id]
            self.acked_message += 1

        print 'acked_message', self.acked_message

    def check_timeout(self):
        for k, v in self.buffer.iteritems():
            if time.time() - v['timestamp'] > self.timeout:
                tuple = dict(v['tuple'])
                # Change message id to avoid premature timeout influence
                tuple['message_id'] = self.message_counter
                self.message_counter += 1

                # Resend the tuple
                self.send_to_children(tuple)
                print 'resend as msgid: ', tuple['message_id']

                # Replace the old record
                del self.buffer[k]
                self.buffer[tuple['message_id']] = {
                    'xor': tuple['tuple_id'] * (len(self.node_conf['children']) % 2),
                    'timestamp': time.time(),
                    'tuple': tuple,
                }

class Bolt(Node):
    def __init__(self, node_conf, crane_conf):
        super(Bolt, self).__init__(node_conf, crane_conf)
        
        self.sock.bind((self.ip, self.node_conf['tuple_port']))

        spout_addr = (self.node_conf['spout']['ip'], self.crane_conf['supervisor']['rpc_port'])
        self.root_machine = xmlrpclib.ServerProxy('http://%s:%d' % spout_addr, allow_none=True)

        # Choose the execute framework to use
        executor_types = {
            'filter': self.filter,
            'transform': self.transform,
            'join': self.join,
            'reduce': self.reduce,
        }
        self.executor = executor_types[self.node_conf['type']]
        if self.node_conf['type'] == 'join':
            self.join_with = open(self.node_conf['join_with'], 'r')
            self.join_on = self.node_conf['join_on']
        elif self.node_conf['type'] == 'reduce':
            self.aggregation = None

        # User-defined function
        self.function = eval(self.node_conf['function'])

        if self.node_conf['sink']:
            self.output = open(self.node_conf['output'], 'w')

    def run(self):
        print self.node_conf
        self.tick = time.time()

        while True:
            tuple = json.loads(self.sock.recvfrom(4096)[0].strip())
            self.executor(tuple)

    def filter(self, tuple):
        old_tuple_id = tuple['tuple_id']
        ack_tuple_ids = [old_tuple_id]

        if self.function(tuple['content']):
            print "successful tuple:", tuple

            if self.node_conf['sink']:
                self.collect(tuple)
            else:
                # Generate a different tuple_id for further process
                new_tuple_id = self.gen_new_tuple_id(old_tuple_id)
                tuple['tuple_id'] = new_tuple_id
                ack_tuple_ids.extend([new_tuple_id] * len(self.node_conf['children']))

                # Propagate to downstream
                self.send_to_children(tuple)

        # Ack the partial xor to the root node
        self.ack(tuple['message_id'], ack_tuple_ids)

    def transform(self, tuple):
        old_tuple_id = tuple['tuple_id']
        ack_tuple_ids = [old_tuple_id]

        # Update the tuple content
        tuple['content'] = self.function(tuple['content'])
        if self.node_conf['sink']:
            self.collect(tuple)
        else:
            # Generate a different tuple_id for further process
            new_tuple_id = self.gen_new_tuple_id(old_tuple_id)
            tuple['tuple_id'] = new_tuple_id
            ack_tuple_ids.extend([new_tuple_id] * len(self.node_conf['children']))

            # Propagate to downstream
            self.send_to_children(tuple)
       
        self.ack(tuple['message_id'], ack_tuple_ids)

    def join(self, tuple):
        old_tuple_id = tuple['tuple_id']
        ack_tuple_ids = [old_tuple_id]

        self.join_with.seek(0)
        for row in self.join_with:
            if tuple['content'][self.join_on] == row[self.join_on]:
                new_tuple_id = self.gen_new_tuple_id(old_tuple_id)
                new_tuple = {
                    'message_id': tuple['message_id'],
                    'tuple_id': new_tuple_id,
                    'content': tuple['content'] + row,
                }

                ack_tuple_ids.extend([new_tuple_id] * len(self.node_conf['children']))
                self.send_to_children(new_tuple)

        self.ack(tuple['message_id'], ack_tuple_ids)

    def reduce(self, tuple):
        assert self.node_conf['sink']

        if self.aggregation == None:
            self.aggregation = tuple['content']
        else:
            self.aggregation = reduce(self.function, [tuple['content'], self.aggregation])

        self.ack(tuple['message_id'], [tuple['tuple_id']])

        # TODO: monitor
        print 'aggregation: ', self.aggregation, time.asctime(), int(time.time() - self.tick)

    def gen_new_tuple_id(self, old):
        new = random.randint(1, (1 << 16) - 1)
        while new == old:
            new = random.randint(1, (1 << 16) - 1)
        return new

    def ack(self, message_id, tuple_ids):
        partial_xor = reduce(operator.xor, tuple_ids)
        self.root_machine.ack(self.node_conf['spout']['id'], message_id, partial_xor)

        print "bolt acked", message_id

    def collect(self, tuple):
        assert self.node_conf['sink']

        self.output.write(str(tuple) + '\n')

if __name__ == '__main__':
    with open('crane_conf.yaml') as f:
        crane_conf = yaml.load(f)

    supervisor = Supervisor(crane_conf)

    rpc_server = SimpleXMLRPCServer((find_local_ip(), crane_conf['supervisor']['rpc_port']), logRequests=True, allow_none=True)
    rpc_server.register_instance(supervisor)
    thread.start_new_thread(rpc_server.serve_forever, ())

    while True:
        pass
