#pylint: disable=C0103,W0105

"""
Here's the algorithm


"""

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WMFactory import WMFactory
from AsyncStageOut.MonitorWorker import MonitorWorker

import logging
import os
from multiprocessing import Pool

result_list = []
current_running = []

def monitor(user, jobids, config):
    """
    Each worker executes this function.
    """
    logging.debug("Trying to start the worker")
    try:
        worker = MonitorWorker(user, jobids , config)
    except Exception as ex:
        logging.debug("Worker cannot be created!: %s" %ex)
        return jobids
    logging.debug("Worker created and init %s" % worker.init)
    if worker.init:
        logging.debug("Starting %s" %worker)
        try:
            worker()
        except Exception as ex:
            logging.debug("Worker cannot start!: %s" %ex)
            return user
    else:
        logging.debug("Worker cannot be initialized!")
    return user


def log_result(result):
    """
    Each worker executes this callback.
    """
    result_list.append(result)
    current_running.remove(result)

class MonitorDaemon(BaseWorkerThread):
    """
    _TransferDaemon_
    Call multiprocessing library to instantiate a MonitorWorker for each job.
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        # self.logger is set up by the BaseWorkerThread, we just set it's level
        self.logger.info('Configuration loaded1')

        self.config = config.Monitor
        try:
            self.logger.setLevel(self.config.log_level)
        except ():
            import self.logger
            self.logger = self.logger.getLogger()
            self.logger.setLevel(self.config.log_level)

        self.logger.debug('Configuration loaded')
        self.dropbox_dir = '%s/dropbox/outputs' % self.config.componentDir
        try:
            os.path.isdir(self.dropbox_dir)
        except():
            self.logger.error('dropbox dir not foud')
            raise
        self.pool = Pool(processes=self.config.pool_size)
        self.factory = WMFactory(self.config.schedAlgoDir, namespace = self.config.schedAlgoDir)


    def algorithm(self, parameters = None):
        """

        """
        files = os.listdir('/data/srv/asyncstageout/v1.0.4/install/asyncstageout/AsyncTransfer/dropbox/outputs/')

        size = len(files)

        if size <= self.config.pool_size:
            users = files
        else:
            sorted_jobs = self.factory.loadObject(self.config.algoName,
                                                  args = [self.config, self.logger, files, self.config.pool_size],
                                                  getFromCache = False, listFlag = True)
            users = sorted_jobs()[:self.config.pool_size]
        self.logger.debug('Number of users in monitor: %s' % len(current_running))

        for user in users:
            files = os.listdir('/data/srv/asyncstageout/v1.0.4/install/asyncstageout/AsyncTransfer/dropbox/outputs/%s'%user)
            if len(files)>0:
                def keys_map(inputDict):
                    """
                    Map function.
                    """
                    return inputDict.split(".")[1]
                jobs = map(keys_map, files)
                if user not in current_running:
                    self.logger.debug('monitoring job IDs: %s' % jobs)
                    current_running.append(user)
                    self.pool.apply_async(monitor,(user,jobs, self.config), callback = log_result)

    def terminate(self, parameters = None):
        """
        Called when thread is being terminated.
        """
        self.pool.close()
        self.pool.join()
