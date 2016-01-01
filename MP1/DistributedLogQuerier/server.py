#!/usr/bin/env python

import yaml
import logging
import socket, SocketServer
import threading
import subprocess
import json

logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s %(levelname)s %(threadName)-10s %(message)s',
    # filename='DLQServer.log',
    # filemode='a+',
)

LOGGER = logging.getLogger('DLQ Server')

def find_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 0))
    return s.getsockname()[0]

def find_local_id():
    ip = find_local_ip()
    with open('conf.yaml') as f:
        server_list = yaml.safe_load(f)['server_list']
    for s in server_list:
        if s['ip'] == ip:
            return s['id']

class LogQueryRequestHandler(SocketServer.BaseRequestHandler):
    """ The request handler to deal with incoming log queries """

    def handle(self):
        request = self.request.recv(1024).strip()
        grep_cmd = json.loads(request)

        handler_thread = threading.current_thread()
        LOGGER.info('Handler thread starts querying [%s]' % ' '.join(grep_cmd))
        
        try: 
            output = subprocess.check_output(grep_cmd)
            # Leave out the ending '\n'
            output = output[:-1]
            line_count = len(output.split('\n'))
            LOGGER.info('Found %d lines.' % line_count)
        except subprocess.CalledProcessError, e:
            LOGGER.info('No matched lines.')
            output = ''
        except Exception, e:
            LOGGER.info(e.strerror)
        finally:
            self.request.sendall(output)

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    """ The server to communicate with clients """
        
def main():
    with open('conf.yaml') as f:
        conf = yaml.safe_load(f)

    HOST, PORT = find_local_ip(), conf['port']
    server = ThreadedTCPServer((HOST, PORT), LogQueryRequestHandler)

    # TODO: 
    server_thread = threading.Thread(target=server.serve_forever())
    LOGGER.info('Server thread starts serving.')
    try:
        server_thread.start()
    except KeyboardInterrupt:
        server.shut_down()
        server.close()
    
if __name__ == '__main__':
    main()