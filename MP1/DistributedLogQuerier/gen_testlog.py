#!/usr/bin/env python

import random
import rstr
import yaml
import server 

def gen_testlog():
    with open('conf.yaml') as f:
        test_conf = yaml.safe_load(f)['test']
    total_size = test_conf['size']
    size_dict = {t : int(total_size * float(f)) for t, f in test_conf['frequency'].iteritems()}
    local_id = server.find_local_id()
    
    # Placeholder
    log_list = ['' for i in xrange(total_size)]
 
    # Gen a line for only this server
    random_idx = int(random.random() * total_size)
    log_list[random_idx] = 'server %d' % local_id

    # Gen a line for specified servers
    if local_id in test_conf['hit_servers']:
        random_idx = int(random.random() * total_size)
        while log_list[random_idx]:
            random_idx = int(random.random() * total_size)
        log_list[random_idx] = 'hit_server'

    # Gen a line for all servers
    random_idx = int(random.random() * total_size)
    while log_list[random_idx]:
        random_idx = int(random.random() * total_size)
    log_list[random_idx] = 'all_server'

    # Gen lines for patterns with different frequencies
    for t, s in size_dict.iteritems():
        cnt = 0
        while cnt < s:
            random_idx = int(random.random() * total_size)
            while log_list[random_idx]:
                random_idx = int(random.random() * total_size)
            log_list[random_idx] = rstr.xeger(test_conf['pattern'][t]) 
            cnt += 1

    # Gen randomly for the rest
    for i in xrange(total_size):
        if not log_list[i]:
            log_list[i] = rstr.xeger(test_conf['pattern']['random']) 
     
    with open('%svm%d.log' % (test_conf['log_path'], local_id), 'w') as f:
        for i in xrange(total_size):
            f.write(log_list[i] + '\n')
         
if __name__ == '__main__':
    gen_testlog()