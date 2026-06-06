import os
import subprocess
import datetime
import logging

# Stop taking new jobs when PBS time remaining is below this threshold (seconds)
CHECK_TIMELEFT = 3600  # 1 hour


def _timedelta_parse(text):
    tokens = text.replace("-", ":").split(":")
    vals = {
        key: float(val)
        for val, key in zip(tokens[::-1], ("seconds", "minutes", "hours", "days"))
    }
    return datetime.timedelta(**vals)


def on_before_task(taskid, cmd):
    jobid = os.getenv("PBS_JOBID", None)
    assert jobid is not None, "PBS_JOBID not found in environment variables."

    proc = subprocess.run(
        ["qstat", "-f", jobid],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    left = 0.0
    for line in proc.stdout.decode("utf-8").splitlines():
        if "Walltime.Remaining" in line:
            val = line.split("=", 1)[1].strip()
            try:
                # some PBS systems report seconds as a plain integer
                left = float(val)
            except ValueError:
                # others report HH:MM:SS
                try:
                    left = _timedelta_parse(val).total_seconds()
                except Exception as e:
                    logging.debug(
                        "on_before_task: exception parsing time string '%s': %s", val, e
                    )
                    left = 0.0
            break

    logging.debug(
        "on_before_task: taskid=%d PBS job %s remaining time: %.2fs",
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
