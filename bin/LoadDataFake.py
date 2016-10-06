#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
"""
from __future__ import print_function
from __future__ import division
import unittest

import time
import string
import random
import urllib
import multiprocessing

from RESTInteractions import HTTPRequests
from ServerUtilities import getHashLfn, generateTaskName, PUBLICATIONDB_STATUSES, encodeRequest, oracleOutputMapping

def submit_docs(thread):
    server = HTTPRequests('cmsweb-testbed.cern.ch', '/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy', '/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy')
    lfnBase = '/store/temp/user/%s/my_cool_dataset-%s/file-%s-%s.root'
    fileDoc = {'id': 'OVERWRITE',
    		'username': 'OVERWRITE',
    		'taskname': 'OVERWRITE',
    		'start_time': 0,
    		'destination': 'T2_CH_CERN',
    		'destination_lfn': 'OVERWRITE',
    		'source': 'T2_CH_CERN',
    		'source_lfn': 'OVERWRITE',
    		'filesize': random.randint(1, 9999),
    		'publish': 0,
    		'transfer_state': 'OVERWRITE',
    		'publication_state': 'OVERWRITE',
    		'job_id': 1,
    		'job_retry_count': 0,
    		'type': 'log',
    		'rest_host': 'cmsweb.cern.ch',
    		'rest_uri': '/crabserver/prod/'}
    ids = []
    users = ['jbalcas', 'mmascher', 'dciangot', 'riahi', 'erupeika', 'sbelforte']  # just random users for tests
    tasks = {}
    totalFiles = 20000

    for i in range(0,totalFiles):
        user = random.choice(users)+str(random.choice(range(0,5)))
        timestamp = time.strftime('%y%m%d_%H%M%S', time.gmtime())
        now = int(time.time())
        # Generate a taskname
        workflowName = ""
        taskname = ""
        if user not in tasks:
            workflowName = "".join([random.choice(string.ascii_lowercase) for _ in range(20)]) + "_" + str(now)
            publicationState = 'NOT_REQUIRED'
        else:
            workflowName = tasks[user]['workflowName']
            publicationState = tasks[user]['publication']
        transferState = 'NEW'
        finalLfn = lfnBase % (user, workflowName, i, random.randint(1, 9999))
        idHash = getHashLfn(finalLfn)
        taskname = '160804_141546:jmsilva_crab_HG1608a-rc1-MinBias_PrivateMC_EventBased-L-T_O-T_P-F_IL-F' 
        fileDoc['id'] = idHash
        fileDoc['job_id'] = i
        fileDoc['username'] = user
        fileDoc['taskname'] = taskname
        fileDoc['start_time'] = int(time.time())
        fileDoc['source_lfn'] = finalLfn
        fileDoc['destination_lfn'] = finalLfn
        fileDoc['transfer_state'] = transferState
        fileDoc['publication_state'] = publicationState

        print(fileDoc)
        server.put('/crabserver/preprod/fileusertransfers', data=encodeRequest(fileDoc))
        print("Query Delay: %s" %(int(time.time())-now))
        time.sleep(0.2)
    return	

jobs = []
for i in range(0,6):
        p = multiprocessing.Process(target=submit_docs,args=(i,))
        jobs.append(p)
        p.start()

