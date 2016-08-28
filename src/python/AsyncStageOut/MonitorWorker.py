#!/usr/bin/env
'''
The MonitorWorker does the following:

'''
import re
import time
import logging
import subprocess
import os
import traceback
import json
from AsyncStageOut import getHashLfn
from AsyncStageOut import getDNFromUserName
from WMCore.Credential.Proxy import Proxy
from WMCore.Services.pycurl_manager import RequestHandler
import StringIO


def getProxy(userdn, group, role, defaultDelegation, logger):
    """
    _getProxy_
    """

    logger.debug("Retrieving proxy for %s" % userdn)
    config = defaultDelegation
    config['userDN'] = userdn
    config['group'] = group
    config['role'] = role
    proxy = Proxy(defaultDelegation)
    proxyPath = proxy.getProxyFilename( True )
    timeleft = proxy.getTimeLeft( proxyPath )
    if timeleft is not None and timeleft > 3600:
        return True, proxyPath
    proxyPath = proxy.logonRenewMyProxy()
    timeleft = proxy.getTimeLeft(proxyPath)
    if timeleft is not None and timeleft > 0:
        return True, proxyPath
    return False, None


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
    logger.debug('Executing : \n command : %s\n output : %s\n error: %s\n retcode : %s' % (command, stdout, stderr, rc))
    return stdout, rc


class MonitorWorker:

    def __init__(self, user, list_job, config):
        """
        store the user and tfc the worker
        """
        self.jobids = list_job
        self.config = config
        self.logger = logging.getLogger('MonitorTransfer-Worker-%s' % (user))
        self.commandTimeout = 1200
        self.uiSetupScript = getattr(self.config, 'UISetupScript', None)
        self.init = True
        if getattr(self.config, 'cleanEnvironment', False):
            self.cleanEnvironment = 'unset LD_LIBRARY_PATH; unset X509_USER_CERT; unset X509_USER_KEY;'
        self.user = user
        self.logger.debug("Trying to get DN for %s" %self.user)
        '''
        try:
            self.userDN = getDNFromUserName(self.user, self.logger)
        except Exception as ex:
            msg = "Error retrieving the user DN"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
            self.init = False
            return
        if not self.userDN:
            self.init = False
            return
        defaultDelegation = {
                                  'logger': self.logger,
                                  'credServerPath' :  self.config.credentialDir,
                                  # It will be moved to be getfrom couchDB
                                  'myProxySvr': 'myproxy.cern.ch',
                                  'min_time_left' : getattr(self.config, 'minTimeLeft', 36000),
                                  'serverDN' : self.config.serverDN,
                                  'uisource' : self.uiSetupScript,
                                  'cleanEnvironment' : getattr(self.config, 'cleanEnvironment', False)
                            }
        if hasattr(self.config, "cache_area"):
            try:
                defaultDelegation['myproxyAccount'] = re.compile('https?://([^/]*)/.*').findall(self.config.cache_area)[0]
            except IndexError:
                self.logger.error('MyproxyAccount parameter cannot be retrieved from %s' % self.config.cache_area)
                pass
        if getattr(self.config, 'serviceCert', None):
                defaultDelegation['server_cert'] = self.config.serviceCert
        if getattr(self.config, 'serviceKey', None):
                defaultDelegation['server_key'] = self.config.serviceKey

        self.logger.debug('cache: %s' %self.config.cache_area)
        '''
        self.userDN = 'testME'
        self.valid = True
        proxy=self.config.opsProxy
        '''
        try:
            self.valid, proxy = getProxy(self.userDN, "", "", defaultDelegation, self.logger)
        except Exception as ex:
            msg = "Error getting the user proxy"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)

        if self.valid:
            self.userProxy = proxy
        else:
            # Use the operator's proxy when the user proxy in invalid.
            # This will be moved soon
            self.logger.error('Did not get valid proxy. Setting proxy to ops proxy')
            self.userProxy = config.opsProxy

        # Proxy management in Couch
        os.environ['X509_USER_PROXY'] = self.userProxy
        '''

    def __call__(self):
        """
        """
        success = False
        while not success:
            success = True
            heade = {"Content-Type ":"application/json"}
            for jid in self.jobids:
                self.jobid=jid.split(".")[1]
                with open(self.config.outputdir+'/%s/Monitor.%s.json' % (self.user, self.jobid)) as json_file:
                   json_data = json.load(json_file)
                self.logger.debug("Connecting to REST FTS for job %s" % json_data)
                '''
                url = self.config.fts_server + '/jobs/%s' % self.jobid
                self.logger.debug("FTS server: %s" % self.config.fts_server)
                buf = StringIO.StringIO()
                datares=''
                try:
                    connection = RequestHandler(config={'timeout': 300, 'connecttimeout' : 300})
                except Exception as ex:
                    msg = str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.debug(msg)
                try:
                    response, datares = connection.request(url, {}, heade, verb='GET', doseq=True, ckey=self.userProxy,
                                                           cert=self.userProxy,
                                                           capath='/etc/grid-security/certificates',
                                                           cainfo=self.userProxy, verbose=False)
                    self.logger.debug('job status: %s' % json.loads(datares)["job_state"])
                except Exception as ex:
                    msg = "Error submitting to FTS: %s " % url
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.debug(msg)
                buf.close()

                if "FINISHED" in json.loads(datares)["job_state"] or json.loads(datares)["job_state"] == "FAILED":
                    self.jobids.remove(jid)
                    reporter = {
                                "LFNs": [],
                                "transferStatus": [],
                                "failure_reason": [],
                                "timestamp": [],
                                "username": ""
                    }
                    lfns = []
                    statuses = []
                    reasons = []
                    timestamps = []
                    user = ''

                    url = self.config.fts_server + '/jobs/%s/files' % self.jobid
                    buf = StringIO.StringIO()
                    try:
                        connection = RequestHandler(config={'timeout': 300, 'connecttimeout' : 300})
                    except Exception as ex:
                        msg = str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.debug(msg)
                    try:
                        response, datares = connection.request(url, {}, heade, verb='GET', doseq=True, ckey=self.userProxy,
                                                               cert=self.userProxy,
                                                               capath='/etc/grid-security/certificates',
                                                               cainfo=self.userProxy, verbose=False)
                        self.logger.debug('Retriving file transfer statuses')
                    except Exception as ex:
                        msg = "Error submitting to FTS: %s " % url
                        msg += str(ex)
                        msg += str(traceback.format_exc())
                        self.logger.debug(msg)
                    buf.close()


                    for source_ in json.loads(datares):
                            source = "/store/temp/user/%s" % source_["source_surl"].split("/user/")[1]
                            state = source_["file_state"]
                            reason = source_["reason"]
                            user = self.user
                            timestamp = source_["finish_time"]
                            timestamps.append(timestamp)
                            lfns.append(source)
                            statuses.append(state)
                            reasons.append(reason)
                            self.logger.debug("ASO doc %s of user %s in state %s (%s)" % \
                                              (getHashLfn(source), user, state, timestamps))
                    
                    reporter["LFNs"] = lfns
                    reporter["transferStatus"] = statuses
                    reporter["username"] = user
                    reporter["reasons"] = reasons
                    reporter["timestamp"] = timestamps
                    '''
                reporter = {
                            "LFNs": [],
                            "transferStatus": [],
                            "failure_reason": [],
                            "timestamp": [],
                            "username": ""
                }
                reporter["LFNs"] = json_data["LFNs"]
                reporter["transferStatus"] = ['Finished' for x in range(len(reporter["LFNs"]))]
                reporter["username"] = self.user
                reporter["reasons"] = ['' for x in range(len(reporter["LFNs"]))]
                reporter["timestamp"] = 10000 
                report_j = json.dumps(reporter)

                self.logger.debug("Creating report %s" % report_j)
                try:
                    if not os.path.exists(self.config.componentDir+"/work/%s" %self.user):
                        os.makedirs(self.config.componentDir+"/work/%s" %self.user)
                    out_file = open(self.config.componentDir+"/work/%s/Reporter.%s.json"%(self.user,self.jobid),"w")
                    out_file.write(report_j)
                    out_file.close()
                    os.remove(self.config.outputdir+'/%s/Monitor.%s.json' %(self.user,self.jobid))
                except Exception as ex:
                    msg="Cannot create fts job report: %s" %ex
                    self.logger.error(msg)

                #else:
                #    success = False
                #    time.sleep(self.config.job_poll_intervall)
