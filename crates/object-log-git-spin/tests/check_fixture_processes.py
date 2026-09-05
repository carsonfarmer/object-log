"""A non-listening, SIGTERM-ignoring child must not survive fixture cleanup."""
import os
import signal
import subprocess
import sys
import time

from check_auth import stop_process_group

child_source = "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready',flush=True); time.sleep(60)"
parent_source = f"""import subprocess,sys,time
child = subprocess.Popen([sys.executable, '-c', {child_source!r}], stdout=subprocess.PIPE)
assert child.stdout.readline() == b'ready\\n'
print(child.pid, flush=True)
time.sleep(60)
"""
process = subprocess.Popen([sys.executable, "-c", parent_source], stdout=subprocess.PIPE, start_new_session=True)
try:
    child = int(process.stdout.readline())
    started = time.monotonic()
    stop_process_group(process, timeout=2)
    elapsed = time.monotonic() - started
    assert 1 <= elapsed < 2, "SIGTERM-ignoring child did not require bounded escalation"
    try:
        os.kill(child, 0)
    except ProcessLookupError:
        pass
    else:
        raise AssertionError("non-listening child survived")
    print("private group drained after escalation; non-listening child gone")
finally:
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)
