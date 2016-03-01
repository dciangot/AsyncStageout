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


def monitor(user, split, list, config):
    """
    Each worker executes this function.
    """
    logging.debug("Trying to start the worker")
    try:
        worker = MonitorWorker(user, split, list, config)
    except Exception as ex:
        logging.debug("Worker cannot be created!: %s" %ex)
        return
    logging.debug("Worker created and init %s" % worker.init)
    if worker.init:
        logging.debug("Starting %s" %worker)
        try:
            worker()
        except Exception as ex:
            logging.debug("Worker cannot start!: %s" %ex)
            return
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

    def algorithm(self, parameters=None):
        """

        """
        files = os.listdir('/data/srv/asyncstageout/v1.0.4/install/asyncstageout/AsyncTransfer/dropbox/outputs/')

        size = len(files)

        if size <= self.config.pool_size:
            users = files
        else:
            sorted_jobs = self.factory.loadObject(self.config.algoName,
                                                  args=[self.config, self.logger, files, self.config.pool_size],
                                                  getFromCache=False, listFlag=True)
            users = sorted_jobs()[:self.config.pool_size]
        self.logger.debug('Number of monitor active threads: %s' % len(current_running))

        for user in users:
            files = os.listdir('/data/srv/asyncstageout/v1.0.4/install/asyncstageout/AsyncTransfer/dropbox/outputs/%s'
                               % user)
            if len(files) > 0:
                files = files[:self.max_jobs_per_user]
                for split in range(0, len(files)//self.jobs_per_thread):
                    user_s = user+'/%s' % split
                    files = files[split*self.jobs_per_thread:(split+1)*self.jobs_per_thread]
                    if user_s not in current_running:
                        self.logger.debug('Starting monitor for %s\'s jobs, split %s' % (user, split))
                        current_running.append(user_s)
                        self.pool.apply_async(monitor, (user, split, files, self.config), callback=log_result)

    def terminate(self, parameters = None):
        """
        Called when thread is being terminated.
        """
        self.pool.close()
        self.pool.join()
