#!/usr/bin/env
'''
The MonitorWorker does the following:

'''
import re
import time
import logging
import subprocess
import os
import datetime
import traceback
import json
from time import strftime
from AsyncStageOut import getHashLfn
from AsyncStageOut import getDNFromUserName
from WMCore.Credential.Proxy import Proxy

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
        return (True, proxyPath)
    proxyPath = proxy.logonRenewMyProxy()
    timeleft = proxy.getTimeLeft( proxyPath )
    if timeleft is not None and timeleft > 0:
        return (True, proxyPath)
    return (False, None)

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

    def __init__(self,user, job, config):
        """
        store the user and tfc the worker
        """
        self.jobids = job 
        self.config = config
        self.logger = logging.getLogger('MonitorTransfer-Worker-%s' % user)
        self.commandTimeout = 1200
        self.uiSetupScript = getattr(self.config, 'UISetupScript', None)
        self.init = True
        if getattr(self.config, 'cleanEnvironment', False):
            self.cleanEnvironment = 'unset LD_LIBRARY_PATH; unset X509_USER_CERT; unset X509_USER_KEY;'
        # TODO: improve how the worker gets a log


        self.user = user
        self.logger.debug("Trying to get DN for %s" %self.user)
        try:
            self.userDN = getDNFromUserName(self.user, self.logger)
        except Exception, ex:
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
        self.valid = False
        try:

            self.valid, proxy = getProxy(self.userDN, "", "", defaultDelegation, self.logger)

        except Exception, ex:

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

    def __call__(self):
        """
        """
        success = False
	while not success:
	 success = True
	 files = os.listdir('/data/srv/asyncstageout/v1.0.4/install/asyncstageout/AsyncTransfer/dropbox/outputs/%s'%self.user)
         def keys_map(inputDict):
                """
                Map function.
                """
                return inputDict.split(".")[1]
         self.jobids = map(keys_map, files)

	 for jid in self.jobids:
	    self.jobid=jid
            command = "glite-transfer-status -l -s %s %s" \
                    % (self.config.fts_server, self.jobid)
            result, rc = execute_command(command, self.logger, self.commandTimeout)
            self.logger.info("job %s status: %s" % (self.jobid, result.split("\n")[0]))
            if "Finished" in result.split("\n")[0] or result.split("\n")[0]=="Failed":
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

                for source_ in result.split("\n"):
                    if "Source:" in source_:
                        source = "/store/temp/user/%s" % source_.split(":      ")[1].split("/user/")[1]
                        index = result.split("\n").index(source_)
                        state = result.split("\n")[index+2].split(":       ")[1]
                        reason = result.split("\n")[index+4].split(":      ")[1]
                        user = source_.split(":      ")[1].split("/user/")[1].split(".")[0]
                        lfns.append(source)
                        statuses.append(state)
                        reasons.append(reason)
                        timestamps.append('')
                        self.logger.debug("ASO doc %s of user %s in state %s" % (getHashLfn(source), user, state))

                reporter["LFNs"] = lfns
                reporter["transferStatus"] = statuses
                reporter["username"] = user
                reporter["reasons"] = reasons
                reporter["timestamp"] = timestamps

                report_j = json.dumps(reporter)

                self.logger.debug("Creating report %s" % report_j)
                try:
                    if not os.path.exists(self.config.componentDir+"/work/%s" %user):
                        os.makedirs(self.config.componentDir+"/work/%s" %user)
                    out_file = open(self.config.componentDir+"/work/%s/Reporter.%s.json"%(user,self.jobid),"w")
                    out_file.write(report_j)
                    out_file.close()
		    os.remove('/data/srv/asyncstageout/v1.0.4/install/asyncstageout/AsyncTransfer/dropbox/outputs/%s/Monitor.%s.json'%(user,self.jobid))
                except Exception as ex:
                    msg="Cannot create fts job report: %s" %ex
                    self.logger.error(msg)
		
	    else:
		success = False	
         time.sleep(self.config.job_poll_intervall)
        return
