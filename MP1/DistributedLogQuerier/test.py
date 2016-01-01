#!/usr/bin/env python

import client
import unittest
import yaml
import os
import sys
import time
import rstr
import logging

logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s %(levelname)s %(threadName)-10s %(message)s',
    # filename='DLQServer.log',
    # filemode='a+',
)

LOGGER = logging.getLogger('DLQ Tester')

class DLCTestCase(unittest.TestCase):
    """ Unit test for DLC """
        
    def test_frequency(self):
        with open('conf.yaml') as f:
            self.conf = yaml.safe_load(f)

        for t, f in self.conf['test']['frequency'].iteritems():
            grep_cmd = ['grep', '-E', self.conf['test']['pattern'][t]]
            c = client.LogQueryClient(grep_cmd)
            c.conf['log_path'] = self.conf['test']['log_path']

            sys.stdout = open('temp.test.out.%s' % t, 'w')
            # sys.stderr = os.devnull
            c.query()
            sys.stdout = sys.__stdout__
            # sys.stderr = sys.__stderr__
            query_result = c.line_dict

            server_num = len(self.conf['server_list'])
            size_of_this_type = int(self.conf['test']['size'] * float(f))
            ground_truth = {i : size_of_this_type for i in xrange(1, server_num + 1)}

            self.assertDictEqual(query_result, ground_truth)

    def test_coverage(self):
        with open('conf.yaml') as f:
            self.conf = yaml.safe_load(f)

        # 1. One server should have matched lines
        grep_cmd = ['grep', 'server 1']
        c = client.LogQueryClient(grep_cmd)
        c.conf['log_path'] = self.conf['test']['log_path']

        sys.stdout = open('temp.test.out.one', 'w')
        # sys.stderr = os.devnull
        c.query()
        sys.stdout = sys.__stdout__
        # sys.stderr = sys.__stderr__
        query_result = c.line_dict

        ground_truth = {1 : 1}

        self.assertDictEqual(query_result, ground_truth)  

        # 2. Some servers should have matched lines
        grep_cmd = ['grep', 'hit_server']
        c = client.LogQueryClient(grep_cmd)
        c.conf['log_path'] = self.conf['test']['log_path']

        sys.stdout = open('temp.test.out.some', 'w')
        # sys.stderr = os.devnull
        c.query()
        sys.stdout = sys.__stdout__
        # sys.stderr = sys.__stderr__
        query_result = c.line_dict

        ground_truth = {i : 1 for i in self.conf['test']['hit_servers']}

        self.assertDictEqual(query_result, ground_truth)  

        # 3. All servers should have matched lines
        grep_cmd = ['grep', 'all_server']
        c = client.LogQueryClient(grep_cmd)
        c.conf['log_path'] = self.conf['test']['log_path']

        sys.stdout = open('temp.test.out.all', 'w')
        # sys.stderr = os.devnull
        c.query()
        sys.stdout = sys.__stdout__
        # sys.stderr = sys.__stderr__
        query_result = c.line_dict

        ground_truth = {i + 1 : 1 for i in xrange(len(self.conf['server_list']))}

        self.assertDictEqual(query_result, ground_truth) 

def test_speed():
        with open('conf.yaml') as f:
            conf = yaml.safe_load(f)

        latency_list = []
        sys.stdout = open('temp.test.out.speed', 'w')
        for i in xrange(conf['test']['speed_test_size']):
            grep_cmd = ['grep', rstr.xeger(r'[a-z]{,4}[.][a-z]{1,3}')]
            c = client.LogQueryClient(grep_cmd)

            tick = time.time()
            c.query()
            tock = time.time()
            
            latency_list.append(tock - tick)
        sys.stdout = sys.__stdout__

        avg_latency = sum(latency_list) / len(latency_list)
        LOGGER.info('Average latency is ' + str(avg_latency)) 

def main():
    test_speed()
    suite = unittest.TestLoader().loadTestsFromTestCase(DLCTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == '__main__':
    main()
