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


def monitor(user, list_job, config):
    """
    Each worker executes this function.
    """
    logging.debug("Trying to start the worker")
    try:
        worker = MonitorWorker(user, list_job, config)
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
        files = os.listdir(self.config.outputdir)

        size = len(files)

        users = files
        self.logger.debug('Number of monitor active threads: %s' % len(current_running))

        for user in users:
            files = os.listdir(self.config.outputdir+'/%s'  % user)
            if len(files) > 0:
                files = files[:self.config.max_jobs_per_user]
                if user not in current_running:
                    self.logger.debug('Starting monitor for %s\'s jobs' % (user))
                    current_running.append(user)
                    self.pool.apply_async(monitor, (user, files, self.config), callback=log_result)

    def terminate(self, parameters = None):
        """
        Called when thread is being terminated.
        """
        self.pool.close()
        self.pool.join()
