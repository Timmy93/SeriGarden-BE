import logging
import threading
import time
import schedule


class Scheduler(threading.Thread):

    def __init__(self, log: logging, recurrence: int, go):
        super().__init__()
        self.recurrence = recurrence
        self.logging = log
        self.go = go
        schedule.every(self.recurrence).minutes.do(self.run_threaded, self.job)
        self.logging.info("Scheduler setupped")
        print("Scheduler setupped")

    def run(self):
        self.logging.info("Scheduler started")
        print("Scheduler started")

        ## TODO Remove this testing function
        self.run_threaded(self.job)#

        while True:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                self.logging.warning("Unknow exception: [" + str(e) + "]")

    @staticmethod
    def run_threaded(job_func):
        job_thread = threading.Thread(target=job_func)
        job_thread.start()

    def job(self):
        self.logging.debug("Starting threaded job")
        recap = self.go.evaluate_watering()
        self.logging.info("Requested " + str(recap.get('actions')) + " watering using " + str(recap.get('water')) + "ml")
