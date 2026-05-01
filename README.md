# parallelcmd

A lightweight Python CLI for queueing and executing shell commands in parallel. Inspired by GNU Parallel, `parallelcmd` provides:

- **Command generation** from argument combinations
- **Concurrent execution** with live output and progress tracking
- **Job management**: inspect, reset, delete, and update queued jobs
- **Flexible workflows**: resume, and scale workers on demand


## Requirements

- Python 3.8+
- Standard library only (no external Python dependencies)

## Quick start

From this directory:

```bash
python3 parallelcmd.py --help
```

Create a job database:

```bash
python3 parallelcmd.py init "echo {}" ::: a b c
```

Run queued jobs with 4 workers:

```bash
python3 parallelcmd.py exec -j 4 --progress
```

Or do both in one command:

```bash
python3 parallelcmd.py run -j 4 --progress "echo {}" ::: a b c
```

If you omit the subcommand entirely, `parallelcmd.py` defaults to `run`:

```bash
python3 parallelcmd.py -j 4 --progress "echo {}" ::: a b c
```

Check status:

```bash
python3 parallelcmd.py check
```

## Command model

`init` builds commands and stores them in `pardb.sqlite` by default. Use `--db <name>` to target `<name>.sqlite`, or set the `PARDB` environment variable.

- `:::` starts an inline argument list.
- `::::` starts an argument list loaded from a file (one value per line; empty lines and `#` comments are ignored).
- `:::: -` reads the argument list from stdin.
- Multiple lists are combined with Cartesian product.
- If the command has no `{}` placeholders, placeholders are appended automatically.
- If no `:::` or `::::` separator is given and stdin is a pipe, stdin lines are used as the argument list automatically.

Example:

```bash
python3 parallelcmd.py init "python train.py --lr {} --seed {}" ::: 1e-3 1e-4 ::: 1 2 3
```

This creates 6 jobs.

## Subcommands

### `init`

Initialize/append job queue.

```bash
python3 parallelcmd.py init [options] <command ...> [ ::: <args ...> ]* [ :::: <argfile ...> ]*
```

Options:
- `-a, --append` append to existing table instead of recreating
- `-f, --force` drop the existing `parjob` table and recreate it
- `--check_dup` skip commands that already exist
- `-v, --verbose`

### `exec`

Execute queued jobs in parallel.

```bash
python3 parallelcmd.py exec [options]
```

Options:
- `-j, --nworkers` number of workers (default: `4`)
- `--id <id ...>` run only these specific job IDs
- `--progress` show aggregate progress line
- `--dashboard` compact live dashboard mode
- `--dryrun` print commands without running
- `-v, --verbose`
- `--timeskip <sec>` throttle displayed output updates
- `--randomorder` fetch pending jobs in random order
- `--prefix <cmd>` prefix each command (example: `srun -N1 -n1`)
- `--max_jobs <n>` max jobs per worker
- `--check_timeleft <sec>` stop taking new jobs when SLURM time left is below threshold
- `--wait <sec>` when no job is available, wait this many seconds and retry instead of exiting (useful when another process is still adding jobs)

### `run`

Initialize and execute in one step (`init` + `exec`).

```bash
python3 parallelcmd.py run [options] <command ...> [ ::: <args ...> ]* [ :::: <argfile ...> ]*
```

Common options include:
- init side: `--append`, `-f/--force`, `--check_dup`
- exec side: `-j/--nworkers`, `--id`, `--progress`, `--dashboard`, `--dryrun`, `--randomorder`, `--prefix`, `--max_jobs`, `--check_timeleft`, `--wait`

### `check`

Inspect queue summary or list all rows.

```bash
python3 parallelcmd.py check [options]
```

Options:
- `-l, --list` list all matching rows instead of the summary
- `--nonzero` filter to only jobs with non-zero exit value
- `--where <sql>` arbitrary SQL `WHERE` clause
- `--like <pattern>` filter by `Command LIKE <pattern>`
- `--id <id ...>` filter by specific job IDs

### `reset`

Reset selected jobs to pending (`Starttime`, `JobRuntime`, `Exitval` set to `NULL`).

```bash
python3 parallelcmd.py reset [--all | --nonzero | --like <pattern> | --id <id ...> | --where <sql>]
```

Options:
- `-a, --all` reset all jobs
- `--nonzero` reset only jobs with non-zero exit value
- `--where <sql>` arbitrary SQL `WHERE` clause
- `--like <pattern>` filter by `Command LIKE <pattern>`
- `--id <id ...>` filter by specific job IDs

Prompts for confirmation before changing rows.

### `delete`

Delete selected jobs.

```bash
python3 parallelcmd.py delete [options]
```

Options:
- `-a, --all` delete all jobs
- `--like <pattern>` filter by SQL LIKE pattern on command text
- `--id <id ...>` filter by job ID(s)

Prompts for confirmation before deleting rows.

### `update`

Find/replace command text for selected jobs.

```bash
python3 parallelcmd.py update [options]
```

Options:
- `--replace "old,new"` find and replace text pair (comma-separated)
- `--like <pattern>` filter by SQL LIKE pattern on command text
- `--id <id ...>` filter by job ID(s)

Prompts for confirmation before updating rows.

## Global options

- `--db <name>` SQLite DB basename; the file on disk is `<name>.sqlite`
- `--db_retries <n>` max retries when SQLite is locked (default: `10`)
- `--log_level {debug,info}` logging level (default: `info`)

## Useful examples

Pipe arguments from stdin (auto-detected when no `:::` or `::::` is given):

```bash
cat cases.txt | python3 parallelcmd.py -j 4 "bash run.sh {}"
seq 10 | python3 parallelcmd.py "echo {}"
```

Pipe stdin explicitly with `:::: -` (combinable with other arg lists):

```bash
cat cases.txt | python3 parallelcmd.py run "bash run.sh {} {}" :::: - ::: seed1 seed2
```

Run scripts from values in a file:

```bash
python3 parallelcmd.py init "bash run_case.sh {}" :::: cases.txt
python3 parallelcmd.py exec -j 8 --progress
```

Use a custom DB file:

```bash
python3 parallelcmd.py --db jobs init "echo {}" ::: x y z
python3 parallelcmd.py --db jobs exec -j 2
```

Keep workers alive while another process appends jobs later:

```bash
python3 parallelcmd.py exec -j 4 --wait 10
python3 parallelcmd.py init -a "echo {}" ::: later1 later2
```

Retry failed jobs only:

```bash
python3 parallelcmd.py reset
python3 parallelcmd.py exec -j 4 --progress
```

Overwrite the queue with a new set of jobs (drop and recreate):

```bash
python3 parallelcmd.py init -f "echo {}" ::: x y z
python3 parallelcmd.py exec -j 4 --progress
```

## Notes

- Job output is streamed to stdout while running.
- Queue state is persisted in SQLite, so you can stop and resume workflows.
- `reset`, `delete`, and `update` are interactive (confirmation required).
- With `--wait`, workers poll for newly appended jobs instead of exiting as soon as the queue is empty.

## Troubleshooting

- **`database is locked`**
	- Usually temporary when multiple workers/processes access SQLite.
	- Retry the command; avoid running multiple `exec` sessions against the same DB at once.

- **No jobs are executed**
	- Check queue state: `python3 parallelcmd.py check --list`.
	- If jobs are already completed or marked in-progress, reset them: `python3 parallelcmd.py reset`.

- **Workers exit before later jobs are appended**
	- Start `exec` with `--wait <seconds>` so workers keep polling.
	- Append work with `init -a ...` from another process or terminal.

- **Unexpected shell behavior / quoting issues**
	- Commands are executed through `bash -c`.
	- Wrap complex commands in quotes and test one command manually before `init`.

- **`--check_timeleft` fails with missing `SLURM_JOB_ID`**
	- This option requires a SLURM job environment.
	- Run inside a SLURM allocation or omit `--check_timeleft`.

- **`update --replace` does not parse as expected**
	- Use exactly one comma-separated pair: `--replace "old,new"`.
	- If your text contains commas, run multiple updates with simpler replacement pairs.

- **Argument file (`::::`) seems ignored**
	- Ensure one argument per line.
	- Blank lines and lines starting with `#` are intentionally skipped.

## FAQ

- **How do I resume after interruption?**
	- Just run `python3 parallelcmd.py exec -j 4 --progress` again.
	- Completed jobs (exit code `0`) stay done; pending jobs continue.

- **How do I retry only failed jobs?**
	- Failed jobs are those with non-zero exit values.
	- Run `python3 parallelcmd.py reset` (default filter resets jobs with `Exitval <> 0`), then run `exec` again.
	- Use `--nonzero` to be explicit: `python3 parallelcmd.py reset --nonzero`.

- **Can I have multiple queues?**
	- Yes. Use different database basenames with `--db`.
	- Example: `python3 parallelcmd.py --db exp1 init ...` then `exec` using the same `--db`.

- **Is it safe to run two `exec` commands on the same DB?**
	- It is not recommended.
	- SQLite coordination can work, but contention/locking increases and behavior is harder to reason about.

- **Can I inspect/edit queued commands before running?**
	- Inspect: `python3 parallelcmd.py check --list`
	- Bulk edit text: `python3 parallelcmd.py update --replace "old,new" --like "%pattern%"`
	- Remove unwanted rows: `python3 parallelcmd.py delete --id 12 13 14`
