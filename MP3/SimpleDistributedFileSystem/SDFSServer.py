#!/usr/bin/env python

from sortedcontainers import SortedDict
import hashlib
import gossiper
import socket
import thread
import yaml
import os
import logging
import time
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import random
import time

logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s %(levelname)s %(threadName)-10s %(message)s',
    # Seperate logs by each instance starting
    filename='SDFS.log.' + str(int(time.time())),
    filemode='w',
)

SDFS_LOGGER = logging.getLogger('SDFS')

def find_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 0))
    return s.getsockname()[0]

class FileTable(object):
    """docstring for FileTable"""
    def __init__(self, myip, server):
        super(FileTable, self).__init__()
        self.ring = SortedDict()
        self.hasher = hashlib.sha224
        self.myhash = self.hash(myip)
        self.add_node(myip)

        self.server = server

    def hash(self, key):
        return self.hasher(key).hexdigest()[:-10]

    def hash_at(self, idx):
        idx %= len(self.ring)
        hash = self.ring.iloc[idx]
        return hash

    def add_node(self, ip):
        hash = self.hash(ip)
        self.ring[hash] = {'ip': ip, 'files': []}

        SDFS_LOGGER.info('After adding %s - %s' % (ip, repr(self.ring)))

    def remove_node(self, failed_list):
        start_time = time.time()
        # this is for debug
        flag  = False
            
        # deep copy failed list because it will be reset soon
        ip_list = list(failed_list)

        # change the order of failed node
        # make sure the smaller id node be handled first
        if len(ip_list) == 2:
            if self.hash(ip_list[0]) == 0 and self.hash(ip_list[1]) == len(self.ring) - 1:
                ip_list[0], ip_list[1] = ip_list[1], ip_list[0]
            elif self.ring.index(self.hash(ip_list[0])) == self.ring.index(self.hash(ip_list[1])) + 1:
                ip_list[0], ip_list[1] = ip_list[1], ip_list[0]

        for ip in ip_list:
            hash = self.hash(ip)
            idx = self.ring.index(hash)

            # if the node is not the direct successor of the failed node, do nothing
            if len(ip_list) == 2 and ip == ip_list[1] and self.hash_at((idx + 2) % len(self.ring)) == self.myhash:
                continue

            if self.hash_at((idx + 1) % len(self.ring)) == self.myhash or (self.hash_at((idx + 2) % len(self.ring)) == self.myhash and len(ip_list) == 2):
                # this is for debug
                flag = True

                heritage = set(self.ring[hash]['files'])
                my_files = set(self.ring[self.myhash]['files'])
                next_files = set(self.ring[self.hash_at(idx + 2)]['files'])

                # determine the 
                to_me = heritage - my_files
                to_next = (heritage & my_files) - next_files
                to_next_next = heritage & my_files & next_files
                replica_list = [list(to_me), list(to_next), list(to_next_next)]
                
                self.ring[self.myhash]['files'].extend(to_me)

                # handle replica
                dest_ip_to_me = self.ring[self.hash_at(self.ring.index(hash) - 1)]['ip']
                dest_ip_to_next = self.ring[self.hash_at(self.ring.index(self.myhash) + 1)]['ip']
                dest_ip_to_next_next = self.ring[self.hash_at(self.ring.index(self.myhash) + 2)]['ip']
                dest_ip_list = [dest_ip_to_me, dest_ip_to_next, dest_ip_to_next_next]
                
                del self.ring[hash]

                self.server.handle_replica(replica_list, dest_ip_list, ip_list)
            
            else:
                del self.ring[hash]
            
            elapsed_time = time.time() - start_time
            if flag:
                print "It takes", elapsed_time, "to handle replica"

    def lookup(self, sdfs_filename):
        hash = self.hash(sdfs_filename)
        idx = self.ring.bisect_left(hash) if self.ring.bisect_left(hash) < len(self.ring) else 0
        ip_list = [self.ring[self.hash_at(idx + i)]['ip'] for i in xrange(3)]
        return ip_list

    def insert(self, sdfs_filename):
        hash = self.hash(sdfs_filename)
        idx = self.ring.bisect_left(hash) if self.ring.bisect_left(hash) < len(self.ring) else 0
        for i in xrange(3):
            node_hash = self.hash_at(idx + i)
            self.ring[node_hash]['files'].append(sdfs_filename)
            
            SDFS_LOGGER.info('Inserted %s to %s' % (sdfs_filename, self.ring[node_hash]['ip']))

    def delete(self, sdfs_filename):
        hash = self.hash(sdfs_filename)
        idx = self.ring.bisect_left(hash) if self.ring.bisect_left(hash) < len(self.ring) else 0
        for i in xrange(3):
            node_hash = self.hash_at(idx + i)
            self.ring[node_hash]['files'].remove(sdfs_filename)
            
            SDFS_LOGGER.info('Deleted %s to %s' % (sdfs_filename, self.ring[node_hash]['ip']))

    def update_replica(self, replica_list, dest_ip_list):
        for i in xrange(3):
            self.ring[self.hash(dest_ip_list[i])]['files'] = list(set(self.ring[self.hash(dest_ip_list[i])]['files'] + replica_list[i]))


    def list_my_store(self):
        print '-' * 5 + 'my files are:'
        for f in self.ring[self.myhash]['files']:
            print f,
        print
        print '-' * 5 + 'that is all'

    def list_file_location(self):
        all_files = set()
        for value in self.ring.values():
            all_files.update(set(value['files']))

        for f in all_files:
            print f + ' is stored at ',
            for value in self.ring.values():
                if f in value['files']:
                    print value['ip'],
            print 


class SDFSServer(object):
    """docstring for SDFSServer"""
    def __init__(self):
        super(SDFSServer, self).__init__()
        with open('sdfs_conf.yaml') as f:
            self.conf = yaml.safe_load(f)
        self.ip = find_local_ip()

        self.block_size = 20000000

        self.filetable = FileTable(self.ip, self)
        self.gossiper = gossiper.Gossiper(self.filetable)

        if not os.path.exists(self.conf['path']):
            os.makedirs(self.conf['path'])

    def put_file(self, local_filename, sdfs_filename):
        start_time = time.time()

        dest_ip_list = self.filetable.lookup(sdfs_filename)
        # Send file to dests
        print 'send to ' + repr(dest_ip_list)
        with open(local_filename, 'rb') as f:
            for dest_ip in dest_ip_list:
                f.seek(0)
                proxy = xmlrpclib.ServerProxy('http://%s:%d' % (dest_ip, self.conf['rpc_port']), allow_none=True)
                while True:
                    data  = f.read(self.block_size)
                    if not data:
                        break
                    proxy.handle_put_file(sdfs_filename, xmlrpclib.Binary(data))

        # Inform everyone the put file
        self.insert_entry(sdfs_filename)
        for id in self.gossiper.member_list.members:
            proxy = xmlrpclib.ServerProxy('http://%s:%d' % (id.split('_')[0], self.conf['rpc_port']), allow_none=True)
            proxy.insert_entry(sdfs_filename)

        elapsed_time = time.time() - start_time
        print "It takes", elapsed_time, "to put file."

    def get_file(self, sdfs_filename, local_filename):
        start_time = time.time()

        source_ip_list = []
        for value in self.filetable.ring.values():
            if sdfs_filename in value['files']:
                source_ip_list.append(value['ip'])

        # get from any available source
        for dest_ip in source_ip_list:
            start_time = time.time()
            try:
                proxy = xmlrpclib.ServerProxy('http://%s:%d' % (dest_ip, self.conf['rpc_port']), allow_none=True)
                file_len = proxy.handle_get_file_len(sdfs_filename)
                times = file_len / self.block_size + 1
                with open(local_filename, 'wb') as f:
                    for x in xrange(times): 
                        binary = proxy.handle_get_file(sdfs_filename, x)
                        f.write(binary.data)

            except Exception, e:
                pass
            else:
                print "Get file ", sdfs_filename, " from ", dest_ip
                break
        
        elapsed_time = time.time() - start_time
        print "It takes ", elapsed_time, " to get file."
        
    def delete_file(self, sdfs_filename):
        source_ip_list = []
        for value in self.filetable.ring.values():
            if sdfs_filename in value['files']:
                source_ip_list.append(value['ip'])

        # delete all the replicas
        for dest_ip in source_ip_list:
            print "to delete file on ", dest_ip
            proxy = xmlrpclib.ServerProxy('http://%s:%d' % (dest_ip, self.conf['rpc_port']), allow_none=True)
            proxy.handle_delete_file(sdfs_filename)

        # Inform everyone the put file
        self.delete_entry(sdfs_filename)
        for id in self.gossiper.member_list.members:
            proxy = xmlrpclib.ServerProxy('http://%s:%d' % (id.split('_')[0], self.conf['rpc_port']), allow_none=True)
            proxy.delete_entry(sdfs_filename)

    def handle_replica(self, replica_list, dest_ip_list, failed_list):
        # for to_me
        dest_ip = dest_ip_list[0]
        proxy = xmlrpclib.ServerProxy('http://%s:%d' % (dest_ip, self.conf['rpc_port']), allow_none=True)
        for replica_to_get in replica_list[0]:
            binary = proxy.handle_get_replica(replica_to_get, self.ip)
            with open(self.conf['path'] + replica_to_get, 'wb') as f:
                f.write(binary.data)
        
        # for to_next
        dest_ip = dest_ip_list[1]
        proxy = xmlrpclib.ServerProxy('http://%s:%d' % (dest_ip, self.conf['rpc_port']), allow_none=True)
        for replica_to_put in replica_list[1]:
            with open(self.conf['path'] + replica_to_put, 'rb') as f:
                f.seek(0)
                proxy.handle_put_replica(replica_to_put, xmlrpclib.Binary(f.read()))

        # for to_next_next
        dest_ip = dest_ip_list[2]
        proxy = xmlrpclib.ServerProxy('http://%s:%d' % (dest_ip, self.conf['rpc_port']), allow_none=True)
        for replica_to_put in replica_list[2]:
            with open(self.conf['path'] + replica_to_put, 'rb') as f:
                f.seek(0)
                proxy.handle_put_replica(replica_to_put, xmlrpclib.Binary(f.read()))

        dest_ip_list[0] = self.ip
        for value in self.filetable.ring.values():
            dest_ip = value['ip']
            if dest_ip in failed_list:
                continue
            else:
                proxy = xmlrpclib.ServerProxy('http://%s:%d' % (dest_ip, self.conf['rpc_port']), allow_none=True)
                proxy.handle_filetable_update(replica_list, dest_ip_list)

    def handle_get_file_len(self, sdfs_filename):
        # with open(self.conf['path'] + sdfs_filename, 'rb') as f:
        return os.path.getsize(self.conf['path'] + sdfs_filename)

    def handle_put_file(self, sdfs_filename, binary):
        with open(self.conf['path'] + sdfs_filename, 'ab') as f:
            f.write(binary.data)

        SDFS_LOGGER.info('Put replica %s here' % (sdfs_filename))

    def handle_get_file(self, sdfs_filename, x):
        with open(self.conf['path'] + sdfs_filename, 'rb') as f:
            while True:
                f.seek(x * self.block_size)
                data  = f.read(self.block_size)
                if not data:
                    break
                # send block back using tcp
                return xmlrpclib.Binary(data)

    def handle_put_replica(self, sdfs_filename, binary):
        with open(self.conf['path'] + sdfs_filename, 'wb') as f:
            f.write(binary.data)

    def handle_get_replica(self, sdfs_filename, source_ip):
        with open(self.conf['path'] + sdfs_filename, 'rb') as f:
            return xmlrpclib.Binary(f.read())

    def handle_delete_file(self, sdfs_filename):
        os.remove(self.conf['path'] + sdfs_filename)

    def handle_filetable_update(self, replica_list, dest_ip_list):
        self.filetable.update_replica(replica_list, dest_ip_list)

    def insert_entry(self, sdfs_filename):
        self.filetable.insert(sdfs_filename)

    def delete_entry(self, sdfs_filename):
        self.filetable.delete(sdfs_filename)

    def run(self):
        self.gossiper.run()
        while True:
            cmd = raw_input('SDFS > ').strip().split()
            head = cmd[0].upper()
            if head == 'PUT':
                self.put_file(cmd[1], cmd[2])
            elif head == 'GET':
                self.get_file(cmd[1], cmd[2])
            elif head == 'DEL':
                self.delete_file(cmd[1])
            elif head == 'STORE':
                self.filetable.list_my_store()
            elif head == 'LIST':
                self.filetable.list_file_location()
            elif head == 'MEM':
                print self.gossiper.member_list
            elif head == 'LEAVE':
                self.gossiper.leave()
            elif head == 'FILE':
                print self.filetable.ring
            else:
                pass

if __name__ == '__main__':
    sdfs_server = SDFSServer()

    rpc_server = SimpleXMLRPCServer((find_local_ip(), 2335), logRequests=True, allow_none=True)
    rpc_server.register_instance(sdfs_server)
    thread.start_new_thread(rpc_server.serve_forever,  ())

    sdfs_server.run()



                    