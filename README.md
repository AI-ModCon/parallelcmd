# parallelcmd

A lightweight Python CLI for queueing and executing shell commands in parallel. Inspired by GNU Parallel, `parallelcmd` provides:

- **Command generation** from argument combinations
- **Concurrent execution** with live output and progress tracking
- **Job management**: inspect, reset, delete, and update queued jobs
- **Flexible workflows**: resume and scale workers on demand

## vs GNU Parallel

| | GNU Parallel | parallelcmd |
|---|---|---|
| **State** | Stateless¹ (fire and forget) | Stateful (SQLite queue persists) |
| **Resume** | Manual (`--joblog` + `--resume`) | Automatic (re-run `exec`) |
| **Job management** | Limited (joblog file) | First-class (`check`, `reset`, `delete`, `update`) |
| **Dependency** | Perl | Python stdlib only |

Use GNU Parallel for one-shot parallel runs. Use parallelcmd when you need to stop, resume, inspect, and selectively retry jobs across sessions — especially for long ML or HPC experiment sweeps.

> ¹ GNU Parallel can be made stateful via `--sqlmaster` / `--sqlworker`, but requires installing the Perl `DBD::SQLite` module separately.

## Installation

Download the single script and make it executable — no pip or dependencies required.

```bash
# wget
wget https://raw.githubusercontent.com/AI-ModCon/parallelcmd/main/parallelcmd.py
chmod +x parallelcmd.py

# curl
curl -O https://raw.githubusercontent.com/AI-ModCon/parallelcmd/main/parallelcmd.py
chmod +x parallelcmd.py
```

You can then run it directly:

```bash
./parallelcmd.py --help
```

Or place it somewhere on your `PATH` (e.g. `~/.local/bin/`) to use it as `parallelcmd.py` from any directory.

## Requirements

- Python 3.8+
- Standard library only (no external Python dependencies)

## Quick start

```bash
python3 parallelcmd.py --help
```

Create a job database:

```bash
python3 parallelcmd.py init "echo {}" ::: a b c
```

Run queued jobs with 4 workers:

```bash
python3 parallelcmd.py exec -j 4
```

Or do both in one command:

```bash
python3 parallelcmd.py run -j 4 "echo {}" ::: a b c
```

If you omit the subcommand entirely, `parallelcmd.py` defaults to `run`:

```bash
python3 parallelcmd.py -j 4 "echo {}" ::: a b c
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

### Placeholders

| Placeholder | Meaning |
|---|---|
| `{}` | Current argument (positional, auto-assigned left to right) |
| `{0}`, `{1}`, … | Explicit positional argument from the Nth `:::` / `::::` list (0-indexed) |
| `{%}` | Worker slot number (0-indexed, stable for the lifetime of `exec`) |
| `{#}` | Job sequence number (the DB `Seq` of the job being run) |

`{}` and `{0}` / `{1}` can be mixed freely. `{%}` and `{#}` are substituted at run time, not at `init` time, so the stored command retains the literal placeholder.

Example — Cartesian product with implicit placeholders:

```bash
python3 parallelcmd.py init "python train.py --lr {} --seed {}" ::: 1e-3 1e-4 ::: 1 2 3
```

This creates 6 jobs.

Example — explicit positional placeholders (same result, order made explicit):

```bash
python3 parallelcmd.py init "python train.py --lr {0} --seed {1}" ::: 1e-3 1e-4 ::: 1 2 3
```

Example — GPU assignment via worker slot:

```bash
python3 parallelcmd.py run -j 4 "CUDA_VISIBLE_DEVICES={%} python train.py --lr {}" ::: 1e-3 1e-4 1e-5 1e-6
```

Each worker holds a fixed slot (0–3), so all its jobs run on the same GPU.

Example — `--zip` to pair lists element-by-element instead of Cartesian product:

```bash
# Without --zip: 4 jobs (a+x, a+y, b+x, b+y)
python3 parallelcmd.py init "cmd {0} {1}" ::: a b ::: x y

# With --zip: 2 jobs (a+x, b+y)
python3 parallelcmd.py init --zip "cmd {0} {1}" ::: a b ::: x y
```

Stops at the shortest list when lengths differ.

## Subcommands

### `init`

Initialize the job queue, or append to an existing one.

```bash
python3 parallelcmd.py init [options] <command ...> [ ::: <args ...> ]* [ :::: <argfile ...> ]*
```

Options:
- `-a, --append` append to existing table instead of recreating
- `-f, --force` drop the existing `parjob` table and recreate it
- `--check_dup` skip commands that already exist
- `--zip` pair argument lists element-by-element instead of Cartesian product
- `-v, --verbose`

### `exec`

Execute queued jobs in parallel.

```bash
python3 parallelcmd.py exec [options]
```

Options:
- `-j, --nworkers <n>` number of workers (default: `4`)
- `--id <id ...>` run only these specific job IDs
- `--progress` show aggregate progress line
- `--bar` show a visual ASCII progress bar (alternative to `--progress`)
- `--eta` append estimated time remaining to the progress or bar line
- `--dashboard` compact live dashboard mode
- `--dryrun` print commands without running; jobs are marked done (exit 0), so run `reset --all` before a real run
- `-v, --verbose`
- `--timeskip <sec>` throttle displayed output updates
- `--randomorder` fetch pending jobs in random order
- `--prefix <cmd>` prefix each command (example: `srun -N1 -n1`)
- `--max_jobs <n>` max jobs per worker
- `--check_timeleft <sec>` stop taking new jobs when SLURM time left is below threshold
- `--wait <sec>` when no job is available, wait this many seconds and retry instead of exiting (useful when another process is still adding jobs)
- `--timeout <sec>` kill a task and move to the next if it runs longer than this many seconds; timed-out jobs are recorded with exit code `124`
- `--retries <n>` retry a failed job up to N times before marking it failed (default: `0`; timed-out jobs are never retried)
- `--halt <n>` stop queuing new jobs after N failures; already-running jobs complete normally
- `--output-dir <dir>` save each job's stdout to `<dir>/<seq>.out`
- `--quiet` suppress per-job output lines (useful with `--progress` or `--bar`)
- `--tag` prefix each output line with the full command instead of the seq ID

### `run`

Initialize and execute in one step (`init` + `exec`).

```bash
python3 parallelcmd.py run [options] <command ...> [ ::: <args ...> ]* [ :::: <argfile ...> ]*
```

Common options include:
- init side: `--append`, `-f/--force`, `--check_dup`, `--zip`
- exec side: `-j/--nworkers`, `--id`, `--progress`, `--bar`, `--eta`, `--dashboard`, `--dryrun`, `--randomorder`, `--prefix`, `--max_jobs`, `--check_timeleft`, `--wait`, `--timeout`, `--retries`, `--halt`, `--output-dir`, `--quiet`, `--tag`

### `check`

Inspect queue summary or list all rows.

```bash
python3 parallelcmd.py check [options]
```

Options:
- `-l, --list` list all matching rows instead of the summary
- `--nonzero` filter to only jobs with non-zero exit value
- `--running` filter to only currently running jobs
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
- `-y, --yes` skip confirmation prompt

Prompts for confirmation before resetting rows (skipped with `-y`).

### `delete`

Delete selected jobs.

```bash
python3 parallelcmd.py delete [options]
```

Options:
- `-a, --all` delete all jobs
- `--like <pattern>` filter by SQL LIKE pattern on command text
- `--id <id ...>` filter by job ID(s)
- `-y, --yes` skip confirmation prompt

Prompts for confirmation before deleting rows (skipped with `-y`).

### `update`

Find/replace command text for selected jobs.

```bash
python3 parallelcmd.py update [options]
```

Options:
- `--replace "old,new"` find and replace text pair (comma-separated)
- `--like <pattern>` filter by SQL LIKE pattern on command text
- `--id <id ...>` filter by job ID(s)
- `-y, --yes` skip confirmation prompt

Prompts for confirmation before updating rows (skipped with `-y`).

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
python3 parallelcmd.py exec -j 8
```

Use a custom DB file:

```bash
python3 parallelcmd.py --db jobs init "echo {}" ::: x y z
python3 parallelcmd.py --db jobs exec -j 2
```

Kill tasks that exceed a time limit and continue to the next job:

```bash
python3 parallelcmd.py exec -j 4 --timeout 300
```

Timed-out jobs are recorded with exit code `124`. Find them with:

```bash
python3 parallelcmd.py check -l --where "Exitval = 124"
```

Reset timed-out jobs to retry with a longer timeout:

```bash
python3 parallelcmd.py reset --where "Exitval = 124"
python3 parallelcmd.py exec -j 4 --timeout 600
```

Keep workers alive while another process appends jobs later:

```bash
python3 parallelcmd.py exec -j 4 --wait 10
python3 parallelcmd.py init -a "echo {}" ::: later1 later2
```

Retry failed jobs only:

```bash
python3 parallelcmd.py reset
python3 parallelcmd.py exec -j 4
```

Overwrite the queue with a new set of jobs (drop and recreate):

```bash
python3 parallelcmd.py init -f "echo {}" ::: x y z
python3 parallelcmd.py exec -j 4
```

## Notes

- Job output is streamed to stdout while running.
- Queue state is persisted in SQLite, so you can stop and resume workflows.
- `reset`, `delete`, and `update` prompt for confirmation by default; pass `-y` to skip.
- With `--wait`, workers poll for newly appended jobs instead of exiting as soon as the queue is empty.

## Aliases

Add these to `~/.bashrc` or `~/.zshrc` to avoid typing the full command each time.
Assumes `parallelcmd.py` is on your `PATH`.

```bash
# parallelcmd aliases
alias pc='parallelcmd.py'

# init
alias pci='parallelcmd.py init'
alias pcia='parallelcmd.py init --append'
alias pcif='parallelcmd.py init --force'

# exec
alias pce='parallelcmd.py exec'
alias pcer='parallelcmd.py exec --randomorder'
alias pcep='parallelcmd.py exec --progress'

# check
alias pck='parallelcmd.py check'
alias pckl='parallelcmd.py check -l'
alias pckf='parallelcmd.py check -l --nonzero'

# reset / delete / update
alias pcr='parallelcmd.py reset'
alias pcra='parallelcmd.py reset --all'
alias pcrf='parallelcmd.py reset --nonzero'
alias pcd='parallelcmd.py delete'
alias pcda='parallelcmd.py delete --all'
alias pcu='parallelcmd.py update'

# reset timed-out jobs
alias pctimeout='parallelcmd.py reset --where "Exitval = 124"'

# exec with N workers and progress  (usage: pcej 8)
pcej() { parallelcmd.py exec -j "$@"; }

# run (init + exec) with common worker counts and progress
pcj4()  { parallelcmd.py run -j 4 "$@"; }
pcj8()  { parallelcmd.py run -j 8 "$@"; }
pcj16() { parallelcmd.py run -j 16 "$@"; }
```

## Troubleshooting

- **`database is locked`**
	- Usually temporary when multiple workers/processes access SQLite.
	- Retry the command; avoid running multiple `exec` sessions against the same DB at once.

- **No jobs are executed**
	- Check queue state: `python3 parallelcmd.py check -l`.
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

- **Some jobs have exit code `124`**
	- These jobs were killed by `--timeout`.
	- Reset and retry them: `python3 parallelcmd.py reset --where "Exitval = 124"`, then re-run `exec` with a larger `--timeout` or without it.

- **`update --replace` does not parse as expected**
	- Use exactly one comma-separated pair: `--replace "old,new"`.
	- If your text contains commas, run multiple updates with simpler replacement pairs.

- **Argument file (`::::`) seems ignored**
	- Ensure one argument per line.
	- Blank lines and lines starting with `#` are intentionally skipped.

## Comparison with GNU Parallel

| Feature | GNU Parallel | parallelcmd |
|---|---|---|
| **Input: inline list** | `:::` | `:::` |
| **Input: file** | `::::` | `::::` |
| **Input: stdin (auto)** | pipe or `-` | pipe (auto-detected when no `:::`) |
| **Input: stdin (explicit)** | `:::: -` | `:::: -` |
| **Input: multiple lists** | Cartesian product | Cartesian product |
| **Input: linked/paired lists** | `--link` | `--zip` |
| **Column split** | `--colsep REGEX` | — |
| **Null delimiter** | `-0` | — |
| **Stop at sentinel** | `-E VALUE` | — |
| **Skip empty lines** | `--no-run-if-empty` | — |
| **Arg substitution: full** | `{}` | `{}` |
| **Arg substitution: no ext** | `{.}` | — |
| **Arg substitution: basename** | `{/}` | — |
| **Arg substitution: dirname** | `{//}` | — |
| **Arg substitution: job #** | `{#}` | `{#}` |
| **Arg substitution: slot #** | `{%}` | `{%}` |
| **Positional substitution** | `{1}`, `{2}`, … | `{0}`, `{1}`, … |
| **Workers** | `-j N` | `-j N` |
| **Load-based throttle** | `--load`, `--noswap`, `--memfree` | — |
| **Nice/priority** | `--nice` | — |
| **Startup delay** | `--delay SEC` | — |
| **Progress bar** | `--progress`, `--eta`, `--bar` | `--progress`, `--bar`, `--eta`, `--dashboard` |
| **Job log** | `--joblog FILE` | SQLite DB (always persisted) |
| **Resume incomplete batch** | `--resume` (via joblog) | re-run `exec` (auto, SQLite state) |
| **Retry failed only** | `--resume-failed` | `reset --nonzero` + `exec` |
| **Retry N times** | `--retries N` | `--retries N` |
| **Skip duplicates** | — | `--check_dup` |
| **Output order** | `-k` / `--keep-order` | — (streamed as-is) |
| **Tag output** | `--tag`, `--tagstring` | `--tag` |
| **Save results to dir** | `--results DIR` | `--output-dir DIR` |
| **Immediate streaming** | `--ungroup` | always streamed |
| **Line buffering** | `--linebuffer` | — |
| **Timeout** | `--timeout DURATION` | `--timeout SEC` |
| **Exit code for timeout** | 124 | 124 |
| **Halt on failure** | `--halt soon/now,fail=N` | `--halt N` |
| **Custom kill signal** | `--termseq` | — |
| **Dry-run** | `--dry-run` | `--dryrun` |
| **Verbose / print cmd** | `--verbose` | `-v` / `--verbose` |
| **Random order** | `--shuf` | `--randomorder` |
| **Interactive confirm** | `--interactive` | — |
| **Command prefix** | `--` (shell) | `--prefix CMD` |
| **SLURM integration** | — | `--check_timeleft SEC` |
| **Remote execution** | `--sshlogin`, `--slf`, `--trc` | — |
| **Distributed file sync** | `--transfer`, `--return`, `--cleanup` | — |
| **Pipe/streaming mode** | `--pipe`, `--block`, `--pipepart` | — |
| **Semaphore mode** | `sem` / `--semaphore` | — |
| **tmux integration** | `--tmux` | — |
| **Multiple queues** | separate invocations | `--db NAME` (named SQLite files) |
| **Inspect queue** | `--joblog` + external tools | `check`, `check -l`, `--where`, `--like` |
| **Edit queued commands** | — | `update --replace` |
| **Delete specific jobs** | — | `delete --id`, `delete --like` |
| **Reset specific jobs** | — | `reset --id`, `reset --where` |
| **Wait for new jobs** | — | `--wait SEC` (keep workers polling) |
| **Max jobs per worker** | — | `--max_jobs N` |
| **External dependencies** | none (Perl) | none (Python stdlib only) |
| **Persistent state** | optional (joblog file) | always (SQLite) |

GNU Parallel is broader for one-shot parallel execution — especially argument substitution, remote/distributed runs, pipe streaming, and output formatting. `parallelcmd` trades those for a persistent job queue with first-class management (inspect, edit, delete, reset by SQL filter) and native SLURM time-limit awareness, making it better suited for long-running experiment pipelines where you need to stop, resume, and selectively retry jobs across sessions.

## FAQ

- **How do I resume after interruption?**
	- Just run `python3 parallelcmd.py exec -j 4` again.
	- Completed jobs (exit code `0`) stay done; pending jobs continue.

- **How do I retry only failed jobs?**
	- Failed jobs are those with non-zero exit values.
	- Run `python3 parallelcmd.py reset` (default filter resets jobs with `Exitval <> 0`), then run `exec` again.
	- Use `--nonzero` to be explicit: `python3 parallelcmd.py reset --nonzero`.

- **What does exit code `124` mean?**
	- The job was killed by `--timeout`. This matches the GNU `timeout` exit code convention.
	- Reset and rerun: `python3 parallelcmd.py reset --where "Exitval = 124"`, then `exec` with a longer `--timeout`.

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
