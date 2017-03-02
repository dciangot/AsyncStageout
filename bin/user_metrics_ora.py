#!/usr/bin/env python
"""
Kibana monitor script for OracleAso
"""
from __future__ import print_function
from __future__ import division

import os
import sys
import json
import time
import pycurl 
import urllib
import urllib2
import httplib
import logging
import datetime
import subprocess
from urlparse import urljoin
from socket import gethostname
from optparse import OptionParser

from RESTInteractions import HTTPRequests
from ServerUtilities import encodeRequest, oracleOutputMapping
from ServerUtilities import TRANSFERDB_STATES, PUBLICATIONDB_STATES

def send(document):
    return requests.post('http://monit-metrics-dev:10012/', data=json.dumps(document), headers={ "Content-Type": "application/json; charset=UTF-8"})

def send_and_check(document, should_fail=False):
    response = send(document)
    assert( (response.status_code in [200]) != should_fail), 'With document: {0}. Status code: {1}. Message: {2}'.format(document, response.status_code, response.text)


if __name__ == "__main__":
    server = HTTPRequests('asotest3.cern.ch',
                          '/home/dciangot/proxy',
                          '/home/dciangot/proxy')


    fileDoc = dict()
    fileDoc['subresource'] = 'activeUsers'
    fileDoc['grouping'] = 0
    fileDoc['asoworker'] = 'asoprod1'

    result = dict()
    try:
        result = server.get('/crabserver/dev/filetransfers',
                         data=encodeRequest(fileDoc))
    except Exception as ex:
        print ("Failed to acquire transfers from oracleDB: %s" % ex)
        sys.exit(0) 
    
    results = oracleOutputMapping(result)

    #print (result)

    metrics = []

    for res in results:
        user = res['username']
        try:
            stat = server.get('/crabserver/dev/filetransfers', 
                              data=encodeRequest({'subresource': 'groupedTransferStatistics', 
                                                  'grouping': 1, 
                                                  'asoworker': 'asoprod1',
                                                  'username': user}))

            stats = oracleOutputMapping(stat)
        except Exception as ex:
            print ("Failed to acquire user stats from oracleDB: %s" % ex)
            sys.exit(0) 
    
        print (stat)

        sources = [x['source'] for x in stats]
        destinations = [x['destination'] for x in stats]

        for src in sources:
            for dst in destinations:
                links = [x for x in stats if x['source']==src and  x['destination']==dst]
                tmp = {
                    'producer': 'crab',
                    'type': 'aso_users',
                    'hostname': gethostname(),
                    'user': user,
                    'destination': dst,
                    'source': src,
                    'transfers':{ 'DONE':{'count':0,'size':0}, 
                                  'ACQUIRED':{'count':0,'size':0}, 
                                  'SUBMITTED':{'count':0,'size':0}, 
                                  'FAILED':{'count':0,'size':0}, 
                                  'RETRY':{'count':0,'size':0} } 
                }
                status=tmp

                for link in links:
                    status['transfers'][TRANSFERDB_STATES[link['transfer_state']]]['count'] = link['nt']
                    tmp['transfers'][TRANSFERDB_STATES[link['transfer_state']]]['count'] = link['nt']

                #print (json.dumps(tmp))
                metrics.append(tmp)
    print (metrics)
    while True:
        try:
            tmp_transfer = open("tmp_transfer","w")
            tmp_transfer.write(json.dumps(metrics))
            tmp_transfer.close()
            break
        except Exception as ex:
            print(ex)
            continue

    try:
        send_and_check(metrics)
    except Exception as ex:
        print(ex)


    sys.exit(0)

