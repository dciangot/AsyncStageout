#!/usr/bin/env
#pylint: disable=C0103,W0105,broad-except,logging-not-lazy,bad-builtin
'''
The TransferWorker does the following:

a. create REST FTS jobs to be submitted
b. submit FTS jobs and create dashboard report
c. update state in the db (couch or oracle, by aso config flag)
d. create json in dropbox folder for FTS monitor

There should be one worker per user.
'''
import os
import re
import json
import time
import urllib
import logging
import datetime
import StringIO
import traceback
import subprocess

from WMCore.WMFactory import WMFactory
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Services.pycurl_manager import RequestHandler

from AsyncStageOut import getProxy
from AsyncStageOut import getHashLfn
from AsyncStageOut import getFTServer
from AsyncStageOut import getDNFromUserName
from AsyncStageOut import getCommonLogFormatter

from RESTInteractions import HTTPRequests
from ServerUtilities import getHashLfn, generateTaskName,\
        PUBLICATIONDB_STATUSES, encodeRequest, oracleOutputMapping

def execute_command(command, logger, timeout):
    """
    _execute_command_
    Funtion to manage commands.
    """
    stdout, stderr, rc = None, None, 99999
    proc = subprocess.Popen(command, shell=True, cwd=os.environ['PWD'],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            stdin=subprocess.PIPE,)
    t_beginning = time.time()
    seconds_passed = 0
    while True:
        if proc.poll() is not None:
            break
        seconds_passed = time.time() - t_beginning
        if timeout and seconds_passed > timeout:
            proc.terminate()
            logger.error('Timeout in %s execution.' % command)
            return stdout, rc
        time.sleep(0.1)
    stdout, stderr = proc.communicate()
    rc = proc.returncode
    logger.debug('Executing : \n command : %s\n output : %s\n \
                 error: %s\n retcode : %s' % (command, stdout, stderr, rc))
    return stdout, rc

class TransferWorker:
    """
    Submit user transfers to FTS
    """
    def __init__(self, user, tfc_map, config):
        """
        store the user transfer info and retrieve user proxy.
        """
        self.user = user[0]
        self.group = user[1]
        self.role = user[2]
        self.tfc_map = tfc_map
        self.config = config
        self.dropbox_dir = '%s/dropbox/outputs' % self.config.componentDir
        logging.basicConfig(level=config.log_level)
        self.logger = logging.getLogger('AsyncTransfer-Worker-%s' % self.user)
        formatter = getCommonLogFormatter(self.config)
        for handler in logging.getLogger().handlers:
            handler.setFormatter(formatter)
        self.pfn_to_lfn_mapping = {}
        self.max_retry = config.max_retry
        self.uiSetupScript = getattr(self.config, 'UISetupScript', None)
        self.submission_command = getattr(self.config, 'submission_command', 'glite-transfer-submit')
        self.cleanEnvironment = ''
        self.userDN = ''
        self.init = True
        if getattr(self.config, 'cleanEnvironment', False):
            self.cleanEnvironment = 'unset LD_LIBRARY_PATH; unset X509_USER_CERT; unset X509_USER_KEY;'
        self.logger.debug("Trying to get DN for %s" % self.user)
        self.userDN = 'imaDummyDNfor/%s' %self.user

        # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.pluginDir, namespace=self.config.pluginDir)
        self.commandTimeout = 1200
        server = CouchServer(dburl=self.config.couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.db = server.connectDatabase(self.config.files_database)
        config_server = CouchServer(dburl=self.config.config_couch_instance, ckey=self.config.opsProxy, cert=self.config.opsProxy)
        self.config_db = config_server.connectDatabase(self.config.config_database)
        self.fts_server_for_transfer = getFTServer("T1_UK_RAL", 'getRunningFTSserver', self.config_db, self.logger)
        self.valid_proxy = True
        self.user_proxy = self.config.opsProxy

        self.oracleDB = HTTPRequests(self.config.oracleDB,
                              self.config.opsProxy,
                              self.config.opsProxy)



    def __call__(self):
        """
        a. makes the RESTFTS job
        b. submits FTS
        c. update status and create dropbox json
        """
        stdout, stderr, rc = None, None, 99999
        fts_url_delegation = self.fts_server_for_transfer.replace('8446', '8443')
        jobs, jobs_lfn, jobs_pfn, jobs_report = self.files_for_transfer()
        self.logger.debug("Processing files for %s " %self.user_proxy)
        if jobs:
	   self.command(jobs, jobs_lfn, jobs_pfn, jobs_report)
        self.logger.info('Transfers completed')
        return

    def source_destinations_by_user(self):
        """
        Get all the destinations for a user
        """
        if self.config.isOracle:
            fileDoc = {}
            fileDoc['asoworker'] = self.config.asoworker
            fileDoc['subresource'] = 'acquiredTransfers'
            fileDoc['grouping'] = 1
            fileDoc['username'] = self.user
            result = []
            try:
                results = self.oracleDB.get(self.config.oracleFileTrans,
                                           data=encodeRequest(fileDoc))
                result = oracleOutputMapping(results)
                res = [[x['source'], x['destination']] for x in result]
            except Exception as ex:
                self.logger.error("Failed to get acquired transfers \
                                  from oracleDB: %s" %ex)
            return res, result
        else:
            query = {'group': True,
                     'startkey':[self.user, self.group, self.role], 'endkey':[self.user, self.group, self.role, {}, {}]}
                     #'stale': 'ok'}
            try:
                sites = self.db.loadView(self.config.ftscp_design, 'ftscp_all', query)
            except:
                return []
            return [[x[4], x[3]] for x in sites['rows']]

    def files_for_transfer(self):
        """
        Process a queue of work per transfer source:destination for a user. Return one
        job per source:destination.
        """
        if self.config.isOracle:
            source_dests, docs = self.source_destinations_by_user()
        else:
            source_dests = self.source_destinations_by_user()
        jobs = {}
        jobs_lfn = {}
        jobs_pfn = {}
        jobs_report = {}
        failed_files = []
        self.logger.info('%s has %s links to transfer on: %s' % (self.user, len(source_dests), str(source_dests)))
        try:
            for (source, destination) in source_dests:
                self.logger.info('%s %s' % (docs[0]['destination'],destination))
                if self.config.isOracle:
                    active_docs = [x for x in docs
                                   if  x['destination']==destination
                                   and x['source']==source
                                   and x['username']==self.user
                                   and x['user_group']==self.group
                                   and x['user_role']==self.role
                                  ]
                    self.logger.info('%s' % active_docs)
                    def map_active(inputdoc):
                        """
                        map active_users
                        """
                        outDict = {}
                        outDict['key'] = [inputdoc['username'],
                                          inputdoc['user_group'],
                                          inputdoc['user_role'],
                                          inputdoc['destination'],
                                          inputdoc['source'],
                                          inputdoc['id']]
                        outDict['value'] = [inputdoc['destination_lfn'], inputdoc['source_lfn']]
                        return outDict
                    active_files = [map_active(x) for x in active_docs]
                    self.logger.debug('%s has %s files to transfer \
                                      from %s to %s' % (self.user,
                                                        len(active_files),
                                                        source,
                                                        destination))
                else:
                    query = {'reduce':False,
                             'limit': self.config.max_files_per_transfer,
                             'key':[self.user, self.group,
                                    self.role, destination, source],
                             'stale': 'ok'}
                    try:
                        active_files = self.db.loadView(self.config.ftscp_design, 'ftscp_all', query)['rows']
                    except:
                        continue
                    self.logger.debug('%s has %s files to transfer from %s to %s' % (self.user, len(active_files),
                                                                                     source, destination))
                new_job = []
                lfn_list = []
                pfn_list = []
                dash_report = []

                # take these active files and make a copyjob entry
                def tfc_map(item):
                    self.logger.debug('Preparing PFNs...')
                    source_pfn = self.apply_tfc_to_lfn('%s:%s' % (source, item['value'][0]))
                    destination_pfn = self.apply_tfc_to_lfn('%s:%s' % (destination, item['value'][1]))
                    self.logger.debug('PFNs prepared...')
                    if source_pfn and destination_pfn and self.valid_proxy:
                        try:
                            acquired_file, dashboard_report = self.mark_acquired([item])
                            self.logger.debug('Files have been marked acquired')
                        except Exception as ex:
                             self.logger.error("%s" % ex)
                             raise
                        if acquired_file:
                            self.logger.debug('Starting FTS Job creation...')
                            # Prepare Monitor metadata
                            lfn_list.append(item['value'][0])
                            pfn_list.append(source_pfn)
                            # Prepare FTS Dashboard metadata
                            dash_report.append(dashboard_report)
                            new_job.append('%s %s' % (source_pfn, destination_pfn))
                            self.logger.debug('FTS job created...')
                        else:
                            pass
                    else:
                        self.mark_failed([item])
                self.logger.debug('Preparing job... %s' % len(active_files))
                map(tfc_map, active_files)
                self.logger.debug('Job prepared...')
                if new_job:
                    jobs[(source, destination)] = new_job
                    jobs_lfn[(source, destination)] = lfn_list
                    jobs_pfn[(source, destination)] = pfn_list
                    jobs_report[(source, destination)] = dash_report
                    self.logger.debug('FTS job ready for submission over  %s ---> %s ...going to next job' % (source, destination))

            self.logger.debug('ftscp input created for %s (%s jobs)' % (self.user, len(jobs.keys())))
            return jobs, jobs_lfn, jobs_pfn, jobs_report
        except:
            self.logger.exception("fail")
            return jobs, jobs_lfn, jobs_pfn, jobs_report

    def apply_tfc_to_lfn(self, file):
        """
        Take a CMS_NAME:lfn string and make a pfn.
        Update pfn_to_lfn_mapping dictionary.
        """
        try:
            site, lfn = tuple(file.split(':'))
        except:
            self.logger.error('it does not seem to be an lfn %s' %file.split(':'))
            return None
        if site in self.tfc_map:
            pfn = self.tfc_map[site].matchLFN('srmv2', lfn)
            # TODO: improve fix for wrong tfc on sites
            try:
                if pfn.find("\\") != -1: pfn = pfn.replace("\\", "")
                if len(pfn.split(':')) == 1:
                    self.logger.error('Broken tfc for file %s at site %s' % (lfn, site))
                    return None
            except IndexError:
                self.logger.error('Broken tfc for file %s at site %s' % (lfn, site))
                return None
            except AttributeError:
                self.logger.error('Broken tfc for file %s at site %s' % (lfn, site))
                return None
            # Add the pfn key into pfn-to-lfn mapping
            if pfn not in self.pfn_to_lfn_mapping:
                self.pfn_to_lfn_mapping[pfn] = lfn
            return pfn
        else:
            self.logger.error('Wrong site %s!' % site)
            return None

    def command(self, jobs, jobs_lfn, jobs_pfn, jobs_report):
        """
        For each job the worker has to complete:
        Delete files that have failed previously
        Create a temporary copyjob file
        Submit the copyjob to the appropriate FTS server
        Parse the output of the FTS transfer and return complete and failed files for recording
        """
        # Output: {"userProxyPath":"/path/to/proxy","LFNs":["lfn1","lfn2","lfn3"],"PFNs":["pfn1","pfn2","pfn3"],"FTSJobid":'id-of-fts-job', "username": 'username'}
        #Loop through all the jobs for the links we have
        failure_reasons = []
        for link, copyjob in jobs.items():
            submission_error = False
            status_error = False
            fts_job = {}
            # Validate copyjob file before doing anything
            self.logger.debug("Valid %s" % self.validate_copyjob(copyjob))
            if not self.validate_copyjob(copyjob): continue

            rest_copyjob = {
                "params":{
                    "bring_online": None,
                    "verify_checksum": False,
                    "copy_pin_lifetime": -1,
                    "max_time_in_queue": self.config.max_h_in_queue,
                    "job_metadata":{"issuer": "ASO"},
                    "spacetoken": None,
                    "source_spacetoken": None,
                    "fail_nearline": False,
                    "overwrite": True,
                    "gridftp": None
                    },
                "files":[]
                }

            for SrcDest in copyjob:
                tempDict = {"sources": [], "metadata": None, "destinations": []}

                tempDict["sources"].append(SrcDest.split(" ")[0])
                tempDict["destinations"].append(SrcDest.split(" ")[1])
                rest_copyjob["files"].append(tempDict)


            fts_job['userProxyPath'] = self.user_proxy
            fts_job['LFNs'] = jobs_lfn[link]
            fts_job['PFNs'] = jobs_pfn[link]
            fts_job['FTSJobid'] = job_id
            fts_job['files_id'] = fileId_list
            fts_job['username'] = self.user
            self.logger.debug("Creating json file %s in %s" % (fts_job, self.dropbox_dir))
            ftsjob_file = open('%s/Monitor.%s.json' % (self.dropbox_dir, fts_job['FTSJobid']), 'w')
            jsondata = json.dumps(fts_job)
            ftsjob_file.write(jsondata)
            ftsjob_file.close()
            self.logger.debug("%s ready." % fts_job)
            # Prepare Dashboard report
            for lfn in fts_job['LFNs']:
                lfn_report = {}
                lfn_report['FTSJobid'] = fts_job['FTSJobid']
                index = fts_job['LFNs'].index(lfn)
                lfn_report['PFN'] = fts_job['PFNs'][index]
                lfn_report['FTSFileid'] = fts_job['files_id'][index]
                lfn_report['Workflow'] = jobs_report[link][index][2]
                lfn_report['JobVersion'] = jobs_report[link][index][1]
                job_id = '%d_https://glidein.cern.ch/%d/%s_%s' % (int(jobs_report[link][index][0]), int(jobs_report[link][index][0]), lfn_report['Workflow'].replace("_", ":"), lfn_report['JobVersion'])
                lfn_report['JobId'] = job_id
                lfn_report['URL'] = self.fts_server_for_transfer
                self.logger.debug("Creating json file %s in %s for FTS3 Dashboard" % (lfn_report, self.dropbox_dir))
                dash_job_file = open('/tmp/Dashboard.%s.json' % getHashLfn(lfn_report['PFN']), 'w')
                jsondata = json.dumps(lfn_report)
                dash_job_file.write(jsondata)
                dash_job_file.close()
                self.logger.debug("%s ready for FTS Dashboard report." % lfn_report)
        return

    def validate_copyjob(self, copyjob):
        """
        the copyjob file is valid when source pfn and destination pfn are not None.
        """
        for task in copyjob:
            if task.split()[0] == 'None' or task.split()[1] == 'None': return False
        return True

    def mark_acquired(self, files=[]):
        """
        Mark the list of files as tranferred
        """
        lfn_in_transfer = []
        dash_rep = ()
        if self.config.isOracle:
            for lfn in files:
                if lfn['value'][0].find('temp') == 7:
                    docId = lfn['key'][5]
                    self.logger.debug("Marking acquired %s" % docId)
                    try:
                        docbyId = self.oracleDB.get('/crabserver/dev/fileusertransfers',
                                                    data=encodeRequest({'subresource': 'getById', 'id': docId}))
                        document = oracleOutputMapping(docbyId, None)[0]
                        fileDoc = {}
                        fileDoc['asoworker'] = self.config.asoworker
                        fileDoc['subresource'] = 'updateTransfers'
                        fileDoc['list_of_ids'] = docId
                        fileDoc['list_of_transfer_state'] = "SUBMITTED"

                        result = self.oracleDB.post('/crabserver/dev/filetransfers',
                                             data=encodeRequest(fileDoc))
                    except Exception as ex:
                        self.logger.error("Error during status update: %s" %ex)

                    lfn_in_transfer.append(lfn)
                    dash_rep = (document['jobid'], document['job_retry_count'], document['taskname'])
                    self.logger.debug("Marked acquired %s of %s" % (docId, lfn))
                    ## TODO: no need of mark good right? the postjob should updated the status in case of direct stageout I think
                    return lfn_in_transfer, dash_rep
        else:
            for lfn in files:
                if lfn['value'][0].find('temp') == 7:
                    docId = getHashLfn(lfn['value'][0])
                    self.logger.debug("Marking acquired %s" % docId)
                    # Load document to get the retry_count
                    try:
                        document = self.db.document(docId)
                    except Exception as ex:
                        msg = "Error loading document from couch"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                        continue
                    if (document['state'] == 'new' or document['state'] == 'retry'):
                        data = {}
                        data['state'] = 'acquired'
                        data['last_update'] = time.time()
                        updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + docId
                        updateUri += "?" + urllib.urlencode(data)
                        try:
                            self.db.makeRequest(uri=updateUri, type="PUT", decode=False)
                        except Exception as ex:
                            msg = "Error updating document in couch"
                            msg += str(ex)
                            msg += str(traceback.format_exc())
                            self.logger.error(msg)
                            continue
                        self.logger.debug("Marked acquired %s of %s" % (docId, lfn))
                        lfn_in_transfer.append(lfn)
                        dash_rep = (document['jobid'], document['job_retry_count'], document['workflow'])
                    else:
                        continue
                else:
                    good_lfn = lfn['value'][0].replace('store', 'store/temp', 1)
                    self.mark_good([good_lfn])
            return lfn_in_transfer, dash_rep

    def mark_good(self, files=[]):
        """
        Mark the list of files as tranferred
        """
        for lfn in files:
            try:
                document = self.db.document(getHashLfn(lfn))
            except Exception as ex:
                msg = "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                continue
            if document['state'] != 'killed' and document['state'] != 'done' and document['state'] != 'failed':
                outputLfn = document['lfn'].replace('store/temp', 'store', 1)
                try:
                    now = str(datetime.datetime.now())
                    last_update = time.time()
                    data = {}
                    data['end_time'] = now
                    data['state'] = 'done'
                    data['lfn'] = outputLfn
                    data['last_update'] = last_update
                    updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + getHashLfn(lfn)
                    updateUri += "?" + urllib.urlencode(data)
                    self.db.makeRequest(uri=updateUri, type="PUT", decode=False)
                except Exception as ex:
                    msg = "Error updating document in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
                try:
                    self.db.commit()
                except Exception as ex:
                    msg = "Error commiting documents in couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
        self.logger.debug("transferred file updated")

    def mark_failed(self, files=[], force_fail=False, submission_error=False, failure_reasons=[]):
        """
        Something failed for these files so increment the retry count
        """
        updated_lfn = []
        for lfn in files:
            data = {}
            if not isinstance(lfn, dict):
                if 'temp' not in lfn:
                    temp_lfn = lfn.replace('store', 'store/temp', 1)
                else:
                    temp_lfn = lfn
            else:
                if 'temp' not in lfn['value'][0]:
                    temp_lfn = lfn['value'][0].replace('store', 'store/temp', 1)
                else:
                    temp_lfn = lfn['value'][0]

            # Load document and get the retry_count
            if self.config.isOracle:
                docId = lfn['key'][5]
                self.logger.debug("Marking failed %s" % docId)
                try:
                    docbyId = self.oracleDB.get('/crabserver/dev/fileusertransfers',
                                                data=encodeRequest({'subresource': 'getById', 'id': docId}))
                except Exception as ex:
                    self.logger.error("Error updating failed docs: %s" %ex)
                    continue
                document = oracleOutputMapping(docbyId, None)[0]
                self.logger.debug("Document: %s" % document)

                fileDoc = {}
                fileDoc['asoworker'] = 'asodciangot1'
                fileDoc['subresource'] = 'updateTransfers'
                fileDoc['list_of_ids'] = docId 

                if force_fail or document['transfer_retry_count'] > self.max_retry:
                    fileDoc['list_of_transfer_state'] = 'FAILED'
                    fileDoc['list_of_retry_value'] = 1
                else:
                    fileDoc['list_of_transfer_state'] = 'RETRY'
                if submission_error:
                    fileDoc['list_of_failure_reason'] = "Job could not be submitted to FTS: temporary problem of FTS"
                    fileDoc['list_of_retry_value'] = 1
                elif not self.valid_proxy:
                    fileDoc['list_of_failure_reason'] = "Job could not be submitted to FTS: user's proxy expired"
                    fileDoc['list_of_retry_value'] = 1
                else:
                    fileDoc['list_of_failure_reason'] = "Site config problem."
                    fileDoc['list_of_retry_value'] = 1

                self.logger.debug("update: %s" % fileDoc)
                try:
                    updated_lfn.append(docId)
                    result = self.oracleDB.post('/crabserver/dev/filetransfers',
                                         data=encodeRequest(fileDoc))
                except Exception as ex:
                    msg = "Error updating document"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue

            else:
                docId = getHashLfn(temp_lfn)
                try:
                    document = self.db.document(docId)
                except Exception as ex:
                    msg = "Error loading document from couch"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
                if document['state'] != 'killed' and document['state'] != 'done' and document['state'] != 'failed':
                    now = str(datetime.datetime.now())
                    last_update = time.time()
                    # Prepare data to update the document in couch
                    if force_fail or len(document['retry_count']) + 1 > self.max_retry:
                        data['state'] = 'failed'
                    else:
                        data['state'] = 'retry'
                    if submission_error:
                        data['failure_reason'] = "Job could not be submitted to FTS: temporary problem of FTS"
                    elif not self.valid_proxy:
                        data['failure_reason'] = "Job could not be submitted to FTS: user's proxy expired"
                    else:
                        data['failure_reason'] = "Site config problem."
                    data['last_update'] = last_update
                    data['retry'] = now

                    # Update the document in couch
                    self.logger.debug("Marking failed %s" % docId)
                    try:
                        updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + docId
                        updateUri += "?" + urllib.urlencode(data)
                        self.db.makeRequest(uri=updateUri, type="PUT", decode=False)
                        updated_lfn.append(docId)
                        self.logger.debug("Marked failed %s" % docId)
                    except Exception as ex:
                        msg = "Error in updating document in couch"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                        continue
                    try:
                        self.db.commit()
                    except Exception as ex:
                        msg = "Error commiting documents in couch"
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.error(msg)
                        continue
            self.logger.debug("failed file updated")
            return updated_lfn

    def mark_incomplete(self, files=[]):
        """
        Mark the list of files as acquired
        """
        self.logger('Something called mark_incomplete which should never be called')
