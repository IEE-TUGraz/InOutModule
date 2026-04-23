import argparse
import datetime
import os
import subprocess
import sys
import time

from printer import Printer

printer = Printer.getInstance()


def _tail_file(filepath, n=20):
    """Return the last n lines of a file, or all if fewer."""
    try:
        with open(filepath, 'r', errors='replace') as f:
            lines = f.readlines()
    except OSError:
        return "(could not read log file)"
    if not lines:
        return "(no output)"
    if len(lines) <= n:
        return ''.join(lines)
    return f"... ({len(lines) - n} lines omitted)\n" + ''.join(lines[-n:])


parser = argparse.ArgumentParser(description='Calls the exact lines from the given file, can be called multiple times.')

parser.add_argument('jobs', type=str, help='Path to the text-file containing the commands to be called.')
parser.add_argument("--spawn", type=int, help='Number of jobs to spawn (if this is specified, it will call itself multiple times)', nargs='?', default=0)
args = parser.parse_args()
printer.information(f"Using jobs from '{args.jobs}'")

if args.spawn >= 1:
    printer.information(f"Spawning {args.spawn} parallel jobs")
    for i in range(args.spawn):
        subprocess.Popen([
            "cmd", "/c", "start", f"Caller {i}: {args.jobs}", "cmd", "/k",
            f"set POST_ACTIVATE_COMMAND=python {os.path.abspath(__file__)} {args.jobs} && call Conda-Activation-Scripts/activate_environment_windows.bat"
        ])
    printer.information(f"Spawned {args.spawn} parallel jobs, exiting... ")
    exit(0)

barrier_waited = {}  # tracks total seconds waited per barrier line index; resets when barrier passes or a job is picked up


def _all_previous_done(jobs_file, lines, barrier_index):
    return all(
        os.path.exists(f"{jobs_file}.finished{j}") or os.path.exists(f"{jobs_file}.error{j}")
        for j in range(barrier_index) if lines[j].strip() not in ("---", "")
    )


def _any_previous_unclaimed(jobs_file, lines, barrier_index):
    return any(
        not os.path.exists(f"{jobs_file}.started{j}")
        and not os.path.exists(f"{jobs_file}.finished{j}")
        and not os.path.exists(f"{jobs_file}.error{j}")
        for j in range(barrier_index) if lines[j].strip() not in ("---", "")
    )


while True:
    with open(args.jobs, 'r') as f:
        lines = f.readlines()

    found_one = False
    restart = False
    for i, line in enumerate(lines):
        started_job_flag = f"{args.jobs}.started{i}"
        finished_job_flag = f"{args.jobs}.finished{i}"
        error_job_flag = f"{args.jobs}.error{i}"
        if line.strip() == "---":
            dot_i = 0
            found_unclaimed = False
            last_len = 0
            while not _all_previous_done(args.jobs, lines, i):
                if _any_previous_unclaimed(args.jobs, lines, i):
                    found_unclaimed = True
                    break
                barrier_waited[i] = barrier_waited.get(i, 0) + 3
                dots = "." * ((dot_i % 5) + 1)
                msg = f"Barrier '---' at line {i}: Checking every 3s, waited {barrier_waited[i]}s already{dots}"
                sys.stdout.write(f"\r{msg}{' ' * max(0, last_len - len(msg))}")
                sys.stdout.flush()
                last_len = len(msg)
                dot_i += 1
                time.sleep(3)
            if last_len:
                sys.stdout.write('\n')
                sys.stdout.flush()
            if found_unclaimed:
                restart = True
                break  # restart outer while loop to pick up the unclaimed job
            barrier_waited.pop(i, None)
            continue  # barrier cleared, keep scanning for next job

        try:
            fd = os.open(started_job_flag, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            continue  # Another worker already claimed this job
        if os.path.exists(finished_job_flag) or os.path.exists(error_job_flag):
            os.close(fd)
            continue

        if barrier_waited:
            barrier_waited.clear()
        start_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with os.fdopen(fd, 'w') as f:
            f.write(f"Command: {line.strip()}\n")
            f.write(f"Started at: {start_datetime}")
        found_one = True
        log_file = f"{args.jobs}.log{i}"
        try:
            printer.information(f"Executing job {i} from '{args.jobs}': {line.strip()}")
            os.system(f"title Job {i} from '{args.jobs}': {line.strip()}")

            start_time = time.time()
            with open(log_file, 'w') as log_f:
                log_f.write(f"Command: {line.strip()}\n")
                log_f.write(f"Started at:  {start_datetime}\n")
                log_f.write(f"{'=' * 60}\n")
                log_f.flush()

                proc = subprocess.Popen(
                    line.strip(),
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
                try:
                    for raw_line in proc.stdout:
                        decoded = raw_line.decode(errors='replace').replace('\r\n', '\n').replace('\r', '\n')
                        sys.stdout.write(decoded)
                        sys.stdout.flush()
                        log_f.write(decoded)
                except KeyboardInterrupt:
                    # CTRL+C also signals Gurobi, which does a graceful shutdown and saves
                    # results. Drain remaining output so the log is complete, then let the
                    # subprocess finish — do not re-raise here.
                    try:
                        for raw_line in proc.stdout:
                            decoded = raw_line.decode(errors='replace').replace('\r\n', '\n').replace('\r', '\n')
                            sys.stdout.write(decoded)
                            sys.stdout.flush()
                            log_f.write(decoded)
                    except KeyboardInterrupt:
                        remaining = proc.stdout.read()
                        if remaining:
                            decoded = remaining.decode(errors='replace').replace('\r\n', '\n').replace('\r', '\n')
                            sys.stdout.write(decoded)
                            log_f.write(decoded)
                proc.wait()
            end_time = time.time()

            if proc.returncode != 0:
                raise RuntimeError(
                    f"Command exited with code {proc.returncode}. "
                    f"See log: {log_file}\n"
                    f"Last output:\n{_tail_file(log_file, 20)}"
                )

            with open(finished_job_flag, 'w') as f:
                f.write(f"Command: {line.strip()}\n")
                f.write(f"Started at:  {start_datetime}\n")
                f.write(f"Finished at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Execution time: {end_time - start_time:.2f} seconds (= {(end_time - start_time) / 60 / 60:.2f} hours)\n")
                f.write(f"Log file: {log_file}\n")

            printer.information(f"Finished job {i} from '{args.jobs}' after {end_time - start_time:.2f} seconds (= {(end_time - start_time) / 60 / 60:.2f} hours).")
        except Exception as e:
            printer.error(f"Error while executing job {i}: {e}")
            with open(error_job_flag, 'w') as f:
                f.write(f"Command: {line.strip()}\n")
                f.write(f"Error while executing job {i} from '{args.jobs}': {e}\n")
                f.write(f"Started at:  {start_datetime}\n")
                f.write(f"Occurred at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Log file: {log_file}\n")
        break

    if not found_one and not restart:
        printer.information(f"No more jobs to execute in '{args.jobs}', exiting.")
        break
