"""Private-netns scheduler. One tc process; monotonic deadlines and ACKed apply times."""
import json
import os
import subprocess
import sys
import time


def validate_plan(plan):
    previous = -1
    for event in plan:
        at = event['atMs']
        if not isinstance(at, int) or at < 0 or at < previous:
            raise ValueError('schedule must be ordered nonnegative integer milliseconds')
        previous = at
        if not event['commands'] or any('\n' in c for c in event['commands']):
            raise ValueError('commands must be nonempty single lines')
    return plan


def main():
    host = os.environ.get('NETBENCH_HOST_NETNS')
    if not host or os.readlink('/proc/self/ns/net') == host:
        raise RuntimeError('Refusing to schedule changes in the host network')
    plan = validate_plan(json.loads(sys.stdin.readline()))
    tc = subprocess.Popen(['stdbuf', '-oL', 'tc', '-batch', '-'], stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, stderr=sys.stderr, text=True)
    try:
        print(json.dumps({'type': 'ready'}), flush=True)
        signal = json.loads(sys.stdin.readline())
        start = int(signal['startNs'])
        for event in plan:
            remaining = (start + event['atMs'] * 1_000_000 - time.monotonic_ns()) / 1e9
            if remaining > 0:
                time.sleep(remaining)
            before = time.monotonic_ns()
            tc.stdin.write('\n'.join(event['commands']) + '\nqdisc show dev lo\n')
            tc.stdin.flush()
            # The show follows successful netlink ACKs for the preceding commands.
            while True:
                line = tc.stdout.readline()
                if not line:
                    raise RuntimeError('tc exited before acknowledging schedule')
                if line.startswith('qdisc clsact'):
                    break
            print(json.dumps({'type': 'applied', 'action': event['action'],
                              'plannedMs': event['atMs'],
                              'submittedMs': (before - start) / 1e6,
                              'appliedMs': (time.monotonic_ns() - start) / 1e6}), flush=True)
        print(json.dumps({'type': 'done'}), flush=True)
    finally:
        tc.stdin.close()
        if tc.wait(timeout=5) != 0:
            raise RuntimeError('tc batch failed')


if __name__ == '__main__':
    main()
