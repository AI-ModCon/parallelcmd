import os
import subprocess
import datetime
import logging

# Stop taking new jobs when SLURM time remaining is below this threshold (seconds)
CHECK_TIMELEFT = 3600  # 1 hour


def _timedelta_parse(text):
    tokens = text.replace("-", ":").split(":")
    vals = {
        key: float(val)
        for val, key in zip(tokens[::-1], ("seconds", "minutes", "hours", "days"))
    }
    return datetime.timedelta(**vals)


def on_before_task(taskid, cmd):
    jobid = os.getenv("SLURM_JOB_ID", None)
    assert jobid is not None, "SLURM_JOB_ID not found in environment variables."

    proc = subprocess.run(
        ["squeue", "-h", "-j", jobid, "-o", "%L"],
        stdout=subprocess.PIPE,
    )
    timestr = proc.stdout.decode("utf-8").strip()

    try:
        left = _timedelta_parse(timestr).total_seconds()
    except Exception as e:
        # "INVALID" when remaining time is less than a few seconds
        logging.debug(
            "on_before_task: exception parsing time string '%s': %s", timestr, e
        )
        left = 0.0

    logging.debug(
        "on_before_task: taskid=%d SLURM job %s remaining time: %.2fs",
        taskid,
        jobid,
        left,
    )

    if left < CHECK_TIMELEFT:
        logging.info(
            "on_before_task: taskid=%d remaining time %.2fs < threshold %.2fs. Stopping worker.",
            taskid,
            left,
            CHECK_TIMELEFT,
        )
        return False

    return True
