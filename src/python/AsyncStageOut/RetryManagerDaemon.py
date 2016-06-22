#!/usr/bin/env python
#pylint: disable-msg=W0613,invalid-name,logging-not-lazy,broad-except
"""
__RetryManagerPoller__

This component does the actualy retry logic. It allows to have
different algorithms.
"""

import time
import urllib
import logging
import datetime
import traceback

from WMCore.WMFactory import WMFactory
from WMCore.WMException import WMException
from WMCore.Database.CMSCouch import CouchServer

from AsyncStageOut.BaseDaemon import BaseDaemon
from RESTInteractions import HTTPRequests
from ServerUtilities import encodeRequest

__all__ = []
def convertdatetime(time_to_convert):
    """
    Convert dates into useable format.
    """
    return int(time.mktime(time_to_convert.timetuple()))

def timestamp():
    """
    generate a timestamp
    """
    time_now = datetime.datetime.now()
    return convertdatetime(time_now)

class RetryManagerException(WMException):
    """
    _RetryManagerException_

    It's totally awesome, except it's not.
    """

class RetryManagerDaemon(BaseDaemon):
    """
    _RetryManagerPoller_

    Polls for Files in CoolOff State and attempts to retry them
    based on the requirements in the selected plugin
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseDaemon.__init__(self, config, 'RetryManager')

        if self.config.isOracle:
            self.oracleDB = HTTPRequests(self.config.oracleDB,
                                         self.config.opsProxy,
                                         self.config.opsProxy)
        else:
            try:
                server = CouchServer(dburl=self.config.couch_instance,
                                     ckey=self.config.opsProxy,
                                     cert=self.config.opsProxy)
                self.db = server.connectDatabase(self.config.files_database)
            except Exception as e:
                self.logger.exception('A problem occured when connecting to couchDB: %s' % e)
                raise
            self.logger.debug('Connected to files DB')

            # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.retryAlgoDir, namespace=self.config.retryAlgoDir)
        try:
            self.plugin = self.factory.loadObject(self.config.algoName, self.config,
                                                  getFromCache=False, listFlag=True)
        except Exception as ex:
            msg = "Error loading plugin %s on path %s\n" % (self.config.algoName,
                                                            self.config.retryAlgoDir)
            msg += str(ex)
            self.logger.error(msg)
            raise RetryManagerException(msg)
        self.cooloffTime = self.config.cooloffTime

    def terminate(self, params):
        """
        Run one more time through, then terminate

        """
        logging.debug("Terminating. doing one more pass before we die")
        self.algorithm(params)


    def algorithm(self, parameters=None):
        """
        Performs the doRetries method, loading the appropriate
        plugin for each job and handling it.
        """
        logging.debug("Running retryManager algorithm")
        if self.config.isOracle:
            fileDoc = {}
            fileDoc['asoworker'] = self.config.asoworker
            fileDoc['subresource'] = 'retryTransfers'
            fileDoc['time_to'] = self.cooloffTime
	    self.logger.debug('fileDoc: %s' %fileDoc)	
            try:
                results = self.oracleDB.post(self.config.oracleFileTrans,
                                             data=encodeRequest(fileDoc))
            except Exception as ex:
                self.logger.error("Failed to get retry transfers \
                                  in oracleDB: %s" %ex)
	    logging.info("Retried files in cooloff: %s" %str(results))	
        else:
            self.doRetries()

    def processRetries(self, files):
        """
        _processRetries_

        Actually does the dirty work of figuring out what to do with jobs
        """
        if len(files) < 1:
            # We got no files?
            return

        propList = []
        fileList = self.loadFilesFromList(recList=files)
        logging.debug("Files in cooloff %s" % fileList)
        # Now we should have the files
        propList = self.selectFilesToRetry(fileList)
        logging.debug("Files to retry %s" % propList)
        now = str(datetime.datetime.now())
        for file in propList:
            # update couch
            self.logger.debug("Trying to resubmit %s" % file['id'])
            try:
                document = self.db.document(file['id'])
            except Exception as ex:
                msg = "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                continue
            if document['state'] != 'killed':
                data = {}
                data['state'] = 'new'
                data['last_update'] = time.time()
                data['retry'] = now
                updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + file['id']
                updateUri += "?" + urllib.urlencode(data)
                try:
                    self.db.makeRequest(uri=updateUri, type="PUT", decode=False)
                except Exception as ex:
                    msg = "Error updating document in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
                self.logger.debug("%s resubmitted" % file['id'])
            else:
                continue
        return

    def loadFilesFromList(self, recList):
        """
        _loadFilesFromList_

        Load jobs in bulk
        """
        all_files = []
        index = 0
        for record in recList:
            all_files.append({})
            all_files[index]['id'] = record['key']
            all_files[index]['state_time'] = record['value']
            index += 1
        return all_files

    def selectFilesToRetry(self, fileList):
        """
        _selectFilesToRetry_

       Select files to retry
       """
        result = []

        if len(fileList) == 0:
            return result
        for file in fileList:
            logging.debug("Current file %s" %file)
            try:
                if self.plugin.isReady(file=file, cooloffTime=self.cooloffTime):
                    result.append(file)
            except Exception as ex:
                msg = "Exception while checking for cooloff timeout for file %s\n" % file
                msg += str(ex)
                logging.error(msg)
                logging.debug("File: %s\n" % file)
                raise RetryManagerException(msg)

        return result

    def doRetries(self):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions
        """
        # Discover files that are in cooloff
        query = {'stale': 'ok'}
        try:
            files = self.db.loadView('AsyncTransfer', 'getFilesToRetry', query)['rows']
        except Exception as e:
            self.logger.exception('A problem occured when contacting \
                                  couchDB to retrieve LFNs: %s' % e)
            return
        logging.info("Found %s files in cooloff" % len(files))
        self.processRetries(files)
