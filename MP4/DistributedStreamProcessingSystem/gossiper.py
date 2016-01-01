#!/usr/bin/env python

import yaml
import logging
import socket
import thread
import json
import time
import random

logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s %(levelname)s %(threadName)-10s %(message)s',
    # Seperate logs by each instance starting
    # filename='log.' + str(int(time.time())),
    # filemode='w',
)

LOGGER = logging.getLogger('Gossiper')

# Synchronize between gossip() and listen()
member_list_lock = thread.allocate_lock()

def find_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 0))
    return s.getsockname()[0]

class MemberList(object):
    """ The member list maintained by gossiper """
    def __init__(self, gossiper, nimbus):
        super(MemberList, self).__init__()
        self.gossiper = gossiper
        self.nimbus = nimbus
        self.introducer_ip = gossiper.conf['introducer']['ip']
        self.threshold = gossiper.conf['threshold']
        # Entry format -- id : {'heartbeat' : heartbeat, 'timestamp' : timestamp, 'status' : status}
        self.members = {}

    def __str__(self):
        delinator_begin = '%sMEMBERLIST BEGINS%s\n' % ('-' * 15, '-' * 15)
        content = ['%s : %6d, %f, %s\n' % (id, info['heartbeat'], info['timestamp'], info['status']) for id, info in self.members.iteritems()]
        delinator_end = '%sMEMBERLIST ENDS%s\n' % ('-' * 15, '-' * 18)
        return delinator_begin + ''.join(content) + delinator_end

    def has_introducer(self):
        for id in self.members:
            if id.split('_')[0] == self.introducer_ip:
                return True
        return False

    def add_introducer(self):
        # Behave like receiving a virtual rumor about introducer
        id = self.introducer_ip
        self.members[id] = {
            'heartbeat': 0,
            'timestamp': 0,
            'status': 'ADDED',
        }
        LOGGER.info('[ADDED INTRODUCER] %s : %s' % (id, str(self.members[id])))

    def del_introducer(self):
        if self.introducer_ip in self.members:
            del self.members[self.introducer_ip]
            LOGGER.info('[DELETED INTRODUCER] %s' % self.introducer_ip)

    def merge(self, rumor):
        for id, heard in rumor.iteritems():
            # Clean the virtual info about introducer, prepare to adopt the real info
            if self.introducer_ip in self.members and id.split('_')[0] == self.introducer_ip:
                self.del_introducer()

            if heard['status'] == 'LEFT':
                # Duplicate LEFT rumor is received
                if not id in self.members or self.members[id]['status'] == 'LEFT':
                    continue
                else:
                    self.members[id].update({
                        'heartbeat' : heard['heartbeat'],
                        'timestamp' : time.time(),
                        'status'    : 'LEFT',
                    })
                    LOGGER.info('[LEFT] %s : %s' % (id, str(self.members[id])))
            elif heard['status'] == 'JOINED':
                # New member is heard
                if not id in self.members:
                    self.members[id] = {
                        'heartbeat' : heard['heartbeat'],
                        'timestamp' : time.time(),
                        'status'    : 'JOINED',
                    }
                    LOGGER.info('[JOINED] %s : %s' % (id, str(self.members[id])))
                    # Add node to nimbus
                    if self.nimbus:
                        self.nimbus.add_machine(id.split('_')[0])
                else:
                    mine = self.members[id]
                    # Update info after hearing a fresher heartbeat
                    if heard['heartbeat'] > mine['heartbeat']:
                        mine.update({
                            'heartbeat' : heard['heartbeat'],
                            'timestamp' : time.time(),
                            'status'    : 'JOINED',
                        })
                        # LOGGER.info('[UPDATED] %s : %s' % (id, str(self.members[id])))
            else:
                LOGGER.info('Unhandled status (%s) in rumor' % heard['status'])


    def refresh(self):
        # Change members' status if necessary
        ## Use items() instead of iteritems(), because dict changes during iteration
        for id, info in self.members.items():
            # Ignore introducer when it's just virtual and may be the only member locally
            if info['status'] == 'ADDED':
                continue

            if info['status'] == 'JOINED' and time.time() - info['timestamp'] > float(self.threshold['suspect']):
                info['status'] = 'SUSPECTED'
                # TODO: figure out why so many suspect
                # LOGGER.info('[SUSPECTED] %s : %s' % (id, self.members[id]))
            elif info['status'] == 'SUSPECTED' and time.time() - info['timestamp'] > float(self.threshold['fail']):
                LOGGER.info('[FAILING] %s : %s' % (id, self.members[id]))
                del self.members[id]
                # Remove node from nimbus
                if self.nimbus:
                    self.nimbus.remove_machine(id.split('_')[0])
            elif info['status'] == 'LEFT' and time.time() - info['timestamp'] > float(self.threshold['forget']):
                LOGGER.info('[FORGETING] %s : %s' % (id, self.members[id]))
                del self.members[id]
                # Remove node from nimbus
                if self.nimbus:
                    self.nimbus.remove_machine(id.split('_')[0])

        if not self.has_introducer() and not self.gossiper.is_introducer():
            # At least, introducer is 'ADDED' locally
            self.add_introducer()

    def get_one_to_gossip_to(self, status=('JOINED', 'ADDED')):
        # Gossip to 'ADDED' node to:
        #   1. initialize my exsitence
        #   2. recover from bad communication
        #   3. help introducer rejoin the group after its failure
        candidates = [id for id, info in self.members.iteritems() if info['status'] in status]
        if not candidates:
            return None
        dest_id = random.choice(candidates)
        dest_ip = dest_id.split('_')[0]
        return dest_ip

    def gen_rumor(self, dest_ip):
        # Rumor rule:
        #   0. timestamp field can be ignored
        #   1. include those are 'JOINED' or 'LEFT'
        #   2. exclude destination, because no one knows better than itself
        #   3. include myself
        rumor_filter = lambda m : m[1]['status'] in ('JOINED', 'LEFT') and m[0].split('_')[0] != dest_ip
        rumor_candidates = {id : {'heartbeat' : info['heartbeat'], 'status' : info['status']} for id, info in self.members.iteritems()}
        rumor = dict(filter(rumor_filter, rumor_candidates.iteritems()))
        rumor[self.gossiper.id] = {
            'heartbeat' : self.gossiper.heartbeat,
            'status'    : self.gossiper.status,
        }
        return rumor

    def count_member(self, status):
        return sum([1 for info in self.members.values() if info['status'] in status])

class Gossiper(object):
    """ Gossiper who maintains the group member list """
    def __init__(self, nimbus=None):
        super(Gossiper, self).__init__()
        with open('gossiper_conf.yaml') as f:
            self.conf = yaml.safe_load(f)
        self.ip = find_local_ip()

        self.id = '%s_%d' % (self.ip, int(time.time()))
        self.heartbeat = 1
        self.timestamp = time.time()
        self.status = 'JOINED'

        # Exclude self in member list
        self.member_list = MemberList(self, nimbus)
        if not self.is_introducer():
            self.member_list.add_introducer()

    def is_introducer(self):
        return self.ip == self.conf['introducer']['ip']

    def heartbeat_once(self, last=False):
        self.heartbeat += 1
        self.timestamp = time.time()
        if last:
            self.status = 'LEFT'

    def gossip(self):
        LOGGER.info('Start gossiping!')
        while True:
            member_list_lock.acquire()
            self.member_list.refresh()

            if self.status == 'TO_LEAVE':
                dest_ip = self.member_list.get_one_to_gossip_to(('JOINED'))
                # Last word to tell if there's any other alive one
                if dest_ip:
                    self.heartbeat_once(last=True)
                    rumor = self.member_list.gen_rumor(dest_ip)
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        sock.sendto(json.dumps(rumor), (dest_ip, self.conf['port']))
                    except Exception:
                        pass
                # Now, I can go peacefully
                self.status = 'AFTER_LEFT'
                break
            elif self.status == 'JOINED':
                dest_ip = self.member_list.get_one_to_gossip_to()
                if dest_ip:
                    self.heartbeat_once()
                    rumor = self.member_list.gen_rumor(dest_ip)
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        sock.sendto(json.dumps(rumor), (dest_ip, self.conf['port']))
                    except Exception:
                        pass
            else:
                LOGGER.info('Unhandled status (%s) of gossiper' % self.status)

            member_list_lock.release()

            time.sleep(float(self.conf['interval']['gossip']))

    def listen(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((self.ip, self.conf['port']))
        LOGGER.info('Start listening!')
        while self.status == 'JOINED':
            rumor = json.loads(sock.recvfrom(1024)[0].strip())
            if rumor:
                member_list_lock.acquire()
                self.member_list.merge(rumor)
                member_list_lock.release()

    def reading(self):
        while True:
            counter = 1 - self.member_list.count_member(['JOINED', 'SUSPECTED'])
            print time.strftime('%Y-%m-%d %H:%M:%S'), "The number of failed member is:", counter

            time.sleep(float(self.conf['interval']['reading']))

    def run(self):
        thread.start_new_thread(self.gossip, ())
        thread.start_new_thread(self.listen, ())
        # thread.start_new_thread(self.reading, ())
        # while True:
        #     pass
        #     cmd = raw_input('list (list members)\nself (my id)\nleave\n')
        #     if cmd == 'list':
        #         print self.member_list
        #     elif cmd == 'self':
        #         print self.id
        #     elif cmd == 'leave':
        #         # Inform gossip() to send last word
        #         self.status = 'TO_LEAVE'
        #         LOGGER.info('Leaving...')
        #         # Wait until gossip() finishes sending last word
        #         while self.status != 'AFTER_LEFT':
        #             pass
        #         LOGGER.info('Left completely.')
        #         break
        #     else:
        #         print 'Wrong command.'

if __name__ == '__main__':
    gossiper = Gossiper()
    gossiper.run()




        