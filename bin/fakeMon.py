import threading
import Queue
import sys
import os
import json
import time

def do_work(in_queue,):
    while True:
        item = in_queue.get()
        # process
        with open("/data/user/105pre3/current/install/asyncstageout/AsyncTransfer/dropbox/outputs/"+item) as json_file:
            json_data = json.load(json_file)

        jobid = item.split(".")[1]

        reporter = {
                    "LFNs": [],
                    "transferStatus": [],
                    "failure_reason": [],
                    "timestamp": [],
                    "username": ""
        }
        reporter["LFNs"] = json_data["LFNs"]
        reporter["transferStatus"] = ['Finished' for x in range(len(reporter["LFNs"]))]
        reporter["username"] = json_data["username"]
        reporter["reasons"] = ['' for x in range(len(reporter["LFNs"]))]
        reporter["timestamp"] = 10000 
        report_j = json.dumps(reporter)
        user = json_data["username"]    

        while True:
            try:
                if not os.path.exists("/data/user/105pre3/current/install/asyncstageout/Monitor/work/%s" %user):
                    os.makedirs("/data/user/105pre3/current/install/asyncstageout/Monitor/work/%s" %user)
                if not os.path.exists("/data/user/105pre3/current/install/asyncstageout/AsyncTransfer/dropbox/inputs/%s" %user):
                    os.makedirs("/data/user/105pre3/current/install/asyncstageout/AsyncTransfer/dropbox/inputs/%s" %user)
                out_file = open("/data/user/105pre3/current/install/asyncstageout/AsyncTransfer/dropbox/inputs/%s/Reporter.%s.json"%(user,jobid),"w")
                out_file.write(report_j)
                out_file.close()
                break
            except Exception as ex:
                msg="Cannot create fts job report: %s" %ex
                print msg
                continue

        os.remove("/data/user/105pre3/current/install/asyncstageout/AsyncTransfer/dropbox/outputs/"+item)
	print ("/data/user/105pre3/current/install/asyncstageout/AsyncTransfer/dropbox/outputs/"+item+" removed.")
        in_queue.task_done()

if __name__ == "__main__":
    work = Queue.Queue()
    results = Queue.Queue()
    total = 20

    # start for workers
    for i in range(0,8):
        t = threading.Thread(target=do_work, args=(work,))
        t.daemon = True
        t.start()

    while True: 	
        # produce data
        for i in os.listdir("/data/user/105pre3/current/install/asyncstageout/AsyncTransfer/dropbox/outputs"):
            work.put(i)

        work.join()
	time.sleep(2)

    sys.exit()
