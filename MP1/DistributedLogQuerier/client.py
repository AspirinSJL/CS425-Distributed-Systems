#!/usr/bin/env python

import yaml
import logging
import socket
import threading
import shlex
import json
import time
import operator
import string

logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s %(levelname)s %(threadName)-10s %(message)s',
    # filename='DLQClient.log',
    # filemode='a+',
)

LOGGER = logging.getLogger('DLQ Client')

class LogQueryThread(threading.Thread):
    """ Worker thread to query the designated server """
    
    def __init__(self, client, server_info, grep_cmd):
        super(LogQueryThread, self).__init__()
        self.client = client
        self.server_info = server_info
        self.grep_cmd = grep_cmd + [self.client.conf['log_path'] + server_info['logfile']]
        self.buffer_size = 1024

    def run(self):
        HOST, PORT = self.server_info['ip'], self.client.conf['port']

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((HOST, PORT))
            sock.sendall(json.dumps(self.grep_cmd))

            # Store results of query as temporary files
            #   so that a mathced line will not be cut into two results
            with open('temp.result.server.%d' % self.server_info['id'], 'w+') as f:
                while True:
                    result = sock.recv(self.buffer_size)
                    # Collect all the results from server
                    if result:
                        f.write(result)
                    # Print the collected results
                    else:
                        # Print each line in a way the original grep does
                        #   but add the log file info ahead it
                        f.seek(0)
                        for line in f.read().split('\n'):
                            if line.strip():
                               print 'From %s: %s' % (self.server_info['logfile'], line)
                        # Report to client the number of matched lines
                        f.seek(0)
                        line_count = len([1 for l in f.read().split('\n') if l])
                        if line_count:
                            self.client.line_dict.setdefault(self.server_info['id'], line_count)
                        break
        except socket.error, e:
            LOGGER.error('Server-%s %s' % (self.server_info['id'], e)) 
        else:
            LOGGER.info('Done with Server-%s' % self.server_info['id'])
        finally:
            sock.close()   

class LogQueryClient():
    """ Client end of log query """

    def __init__(self, grep_cmd):
        with open('conf.yaml') as f:
            self.conf = yaml.safe_load(f)

        self.grep_cmd = grep_cmd
        self.line_dict = {}

    def query(self):
        # Designate a seperate worker thread to query each server 
        threads = []
        for server_info in self.conf['server_list']:
            worker_thread = LogQueryThread(self, server_info, self.grep_cmd)
            worker_thread.start()
            threads.append(worker_thread)

        # Do not promot to next query until all threads in this one have finished
        #   A.K.A. barrier
        for t in threads:
            t.join()

        # Do statistics of the results
        self.total_line = reduce(operator.add, self.line_dict.values()) if self.line_dict else 0
        LOGGER.info('Totally found %d lines.' % self.total_line)
        LOGGER.info('Each server: ' + str(self.line_dict))

def main():
    while True:
        grep_cmd = shlex.split(raw_input('DLQ > '))
        client = LogQueryClient(grep_cmd)
        client.query()

if __name__ == '__main__':
        main()    
        