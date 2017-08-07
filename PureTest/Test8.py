import atexit
from pytz import utc
import os


import datetime
import time

# Initial reporting background job
from apscheduler.schedulers.background import BackgroundScheduler

import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def init():
    sched = BackgroundScheduler(daemon=True, timezone=utc, misfire_grace_time=None)
    atexit.register(lambda: sched.shutdown(wait=False))
    sched.configure(misfire_grace_time=None)
    sched.start()
    # jjj = sched.get_job('barn2_snapshot_1')

    return sched

def job(sched, job_id):
    if sched is not None and job_id is not None:
        job = sched.get_job(job_id)
        executor = sched._lookup_executor(job.executor)
        start_waiting = datetime.utcnow()
        while executor._instances[job.id] > 1:
            current_waiting = datetime.utcnow()
            if (current_waiting - start_waiting).seconds > cfg.get('snapshot', 'cron_misfire_grace_mins') * 60:
                # Already waited more than cron_misfire_grace_mins. Will stop current job instance right away
                return

            time.sleep(15 * 60)

if __name__ == '__main__':
    sched = init()

    pid = os.getpid()
    jj = sched.add_job(
        job,
        'cron',
        id='barn2_snapshot_%s' % pid,
        name='BARN2 Snapshot Job',
        day_of_week='*',
        hour='*',
        minute='*',
        second='30',
        max_instances=2,
        args=(sched, 'barn2_snapshot_%s' % pid)
    )

    while True:
        time.sleep(1)


