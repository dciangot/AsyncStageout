"""
asoOracle test API
"""
#pylint: disable=C0103,W0105
#!/usr/bin/env python

# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division


from RESTInteractions import HTTPRequests
from ServerUtilities import encodeRequest, oracleOutputMapping

server = HTTPRequests('mmascher-gwms.cern.ch',
                      '/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy',
                      '/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy')


fileDoc = {}
fileDoc['asoworker'] = 'asodciangot1'
fileDoc['subresource'] = 'acquireTransfers'

result = server.post('/crabserver/dev/filetransfers',
                     data=encodeRequest(fileDoc))


print(result)
"""

fileDoc = {}
fileDoc['asoworker'] = 'asodciangot1'
fileDoc['subresource'] = 'acquiredTransfers'
fileDoc['grouping'] = 0

result = server.get('/crabserver/dev/filetransfers',
                    data=encodeRequest(fileDoc))

#print(oracleOutputMapping(result))

ids = [str(x['id']) for x in oracleOutputMapping(result)]

fileDoc = {}
fileDoc['subresource'] = 'groupedTransferStatistics'
fileDoc['grouping'] = 0

result = server.get('/crabserver/dev/filetransfers',
                    data=encodeRequest(fileDoc))
"""
#print (oracleOutputMapping(result))
fileDoc = {}
fileDoc['asoworker'] = 'asodciangot1'
fileDoc['subresource'] = 'updateTransfers'
fileDoc['list_of_ids'] = '64856469f4602d45c26a23bc6d3b94c3d5f47ba5143ddf84f8b3c1e4'
fileDoc['list_of_transfer_state'] = 4
fileDoc['retry_value'] = 1
fileDoc['fail_reason'] = 'fail_reason'
#print(encodeRequest(fileDoc))

result = server.post('/crabserver/dev/filetransfers',
                     data=encodeRequest(fileDoc))
print(result)


