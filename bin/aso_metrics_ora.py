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


def check_availability():
    """put here your availability logic, """
    return 1

def generate_xml(input):
    from xml.etree.ElementTree import Element, SubElement, tostring
    from pprint import pprint
    xmllocation = './ASO_XML_Report.xml'
    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s %(message)s", datefmt="%a, %d %b %Y %H:%M:%S %Z(%z)")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    root = Element('serviceupdate')
    root.set( "xmlns",  "http://sls.cern.ch/SLS/XML/update")
    child = SubElement(root, "id")  

    # change this to a name which you will use in kibana queries(for example vocms031 or any other name)
    # or just uncomment next line to use the hostname of the machine which is running this script
    # child.text = gethostname().split('.')[0]
    child.text = "oramon-test" 

    fmt = "%Y-%m-%dT%H:%M:%S%z"
    now_utc = datetime.datetime.now().strftime(fmt)
    child_timestamp = SubElement(root, "timestamp")
    child_timestamp.text = str(now_utc)

    child_status = SubElement(root,"status")
    
    # when you have a way to calculate the availability for your service
    # change the function check_availability, for now it will
    # always return 1(available)
    if check_availability() == 1:
        # This means that everything is fine
        child_status.text = "available"
    else:
        child_status.text = "degraded"

    # now put all numeric values her
    data = SubElement(root, "data")

    for key in input.keys():
      if isinstance(input[key],dict):
         for skey in input[key]:
           nName="%s_%s"%(key,skey)
           nValue=input[key][skey]
           numericval = SubElement(data, "numericvalue")
           numericval.set("name",nName)
           numericval.text = str(nValue)

    temp_xmllocation = xmllocation + ".temp"
    try:
      with open(temp_xmllocation, 'w') as f:
        f.write(tostring(root))
        os.system('mv %s %s' % (temp_xmllocation, xmllocation))
    except Exception, e:
      logger.debug(str(e))

    # push the XML to elasticSearch 
    cmd = "curl -i -F file=@%s xsls.cern.ch"%xmllocation
    try:
        pu = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except Exception, e:
        logger.debug(str(e))





if __name__ == "__main__":
    server = HTTPRequests('mmascher-gwms.cern.ch',
                          '/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy',
                          '/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy')

    result = server.get('/crabserver/dev/filetransfers', data=encodeRequest({'subresource': 'groupedTransferStatistics', 'grouping': 0}))

    results = oracleOutputMapping(result)


    status = {'timestamp':"", 'transfers':{}, 'publications':{}}
    for doc in results:
        if doc['aso_worker']=="asodciangot1": 
            status['transfers'][TRANSFERDB_STATES[doc['transfer_state']]] = doc['nt']

    result = server.get('/crabserver/dev/filetransfers', data=encodeRequest({'subresource': 'groupedPublishStatistics', 'grouping': 0}))

    results = oracleOutputMapping(result)

    for doc in results:
        if doc['aso_worker']=="asodciangot1": 
            status['publications'][TRANSFERDB_STATES[doc['publication_state']]] = doc['nt']

    print (status)

    generate_xml(status)

    sys.exit(0)

