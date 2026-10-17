#!/usr/bin/env python3
"""Discover and invoke every Java/Scala main; retain exit codes and full logs.

Run with JAVA_HOME pointing to JDK 17 after compiling and generating classpaths
(see docs/main-execution-status.md). A timeout is never a passing result.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]


def discover():
    entries = []
    for module in ('realtime_dw', 'rt_rule_mk', 'redis_demo', 'batch_jobs'):
        for path in sorted((ROOT / module / 'src').rglob('*')):
            if path.suffix not in ('.java', '.scala'):
                continue
            text = path.read_text()
            # Ignore commented-out declarations when building the inventory.
            text = re.sub(r'/\*.*?\*/|//[^\n]*', '', text, flags=re.S)
            if not re.search(r'\b(?:public\s+static|static\s+public)\s+void\s+main\s*\(|\bdef\s+main\s*\(', text):
                continue
            package = re.search(r'\bpackage\s+([\w.]+)', text).group(1)
            entries.append({'module': module, 'class': package + '.' + path.stem,
                            'source': str(path.relative_to(ROOT))})
    return entries


def run(entry, output, timeout):
    logfile = output / (entry['class'] + '.log')
    command = ['bash', str(ROOT / 'scripts/run-main.sh'), entry['module'], entry['class']]
    env = dict(os.environ, SKIP_MAIN_COMPILE='true')
    started = time.monotonic()
    with logfile.open('w') as log:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        timed_out = False
        environment_error = None
        while process.poll() is None:
            elapsed = time.monotonic() - started
            text = logfile.read_text(errors='replace')
            match = re.search(r'No resolvable bootstrap urls given in bootstrap.servers|java\.net\.UnknownHostException: [^\n]+|Connect to doris http://[^\n]+ failed\.', text)
            if match:
                environment_error = match.group(0)
                break
            if elapsed >= timeout:
                timed_out = True
                break
            time.sleep(0.25)
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        code = process.returncode
    entry = dict(entry, exit_code=code, timed_out=timed_out, environment_error=environment_error,
                 seconds=round(time.monotonic() - started, 2), log=str(logfile.relative_to(ROOT)))
    print(f"{entry['class']}: exit={code}, timeout={timed_out}, {entry['seconds']}s", flush=True)
    return entry


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--timeout', type=int, default=25)
    parser.add_argument('--only', help='Regular expression matching class names')
    parser.add_argument('--output', default='.artifacts/main-audit/after')
    parser.add_argument('--resume', action='store_true', help='Keep entries already recorded in this output directory')
    args = parser.parse_args()
    output = ROOT / args.output
    output.mkdir(parents=True, exist_ok=True)
    entries = discover()
    if args.only:
        entries = [e for e in entries if re.search(args.only, e['class'])]
    previous = output / 'results.json'
    results = json.loads(previous.read_text())['results'] if args.resume and previous.exists() else []
    completed = {e['class'] for e in results}
    for entry in entries:
        if entry['class'] in completed:
            continue
        results.append(run(entry, output, args.timeout))
        (output / 'results.json').write_text(json.dumps({
            'recorded_at': datetime.datetime.now().astimezone().isoformat(),
            'results': results}, ensure_ascii=False, indent=2) + '\n')


if __name__ == '__main__':
    main()
