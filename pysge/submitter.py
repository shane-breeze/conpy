import os
import re
import logging
import time
from builtins import input
from multiprocessing import Pool
from tqdm import tqdm

from .utils import run_command

logger = logging.getLogger(__name__)

class SGETaskSubmitter(object):
    submit_command = 'qsub -cwd -V -e /dev/null -o /dev/null -t 1-{njobs}:1 {job_opts} {executable}'
    regex_submit = re.compile('Your job-array (?P<jobid>[0-9]+)\.1-[0-9]+:1 \(".*"\) has been submitted')
    def __init__(self, job_options):
        self.job_options = job_options
        self.jobid_tasks = {}

    def submit_tasks(self, tasks, dryrun=False, request_user_input=False):
        if tasks is None or len(tasks) <= 0:
            return

        curdir = os.getcwd()
        njobs = len(tasks)
        os.chdir(os.path.dirname(tasks[0]))
        executable = run_command("which pysge_worker.sh")[0].decode("utf-8")

        job_opts = self.job_options
        if request_user_input:
            job_opts = input(
                "Using job options '{}'. Insert new options or nothing to use "
                "the default\n:"
            )
            job_opts = job_opts if job_opts != "" else self.job_options

        cmd = self.submit_command.format(
            executable=executable, njobs=njobs, job_opts=job_opts,
        )
        if not dryrun:
            out, err = run_command(cmd)
            match = self.regex_submit.search(out.decode("utf-8"))
            jobid = int(match.group("jobid"))
            logger.info('Submitted {}.1-{}:1'.format(jobid, njobs))
        else:
            print(cmd)
            jobid = 0

        os.chdir(curdir)

        for aid in range(njobs):
            arrayid = aid+1
            self.jobid_tasks['{}.{}'.format(jobid, arrayid)] = tasks[aid]

    def killall(self):
        jids = []
        for jobid, _ in self.jobid_tasks.items():
            tjid = jobid.split(".")[0]
            if tjid not in jids:
                jids.append(tjid)
        cmd = "qdel {}".format(" ".join(jids))
        run_command(cmd)

class MPTaskSubmitter(object):
    def submit_tasks(self, tasks, ncores=4, sleep=1):
        if tasks is None or len(tasks) <= 0:
            return

        results = []
        pool = Pool(processes=ncores)
        pbar = tqdm(total=len(tasks), desc="Finished")

        try:
            for task in tasks:
                results.append(
                    pool.apply_async(task['task'], task['args'], task['kwargs'])
                )

            nremaining = len(tasks)
            while nremaining > 0:
                nfinished = sum(r.ready() for r in results)
                nremaining = len(tasks) - nfinished
                pbar.n = nfinished
                pbar.refresh()
                time.sleep(sleep)

            results = [r.get() for r in results]
        except KeyboardInterrupt:
            pool.terminate()
            results = []
        pool.close()
        pbar.close()
        print("")

        return {i: r for i, r in enumerate(results)}
