"""Local process-group regression; no Docker or Spin required.

Usage: python3 check_process_cleanup.py [check_shallow.py check_partial.py]
Extract only stop() so the provider fixture's top-level code never executes.
"""
import ast
import errno
import os
import pathlib
import signal
import socket
import subprocess
import sys
import tempfile
import time

for name in sys.argv[1:] or ["check_shallow.py"]:
    path = pathlib.Path(__file__).parent / name
    tree = ast.parse(path.read_text())
    function = next(node for node in tree.body
                    if isinstance(node, ast.FunctionDef) and node.name == "stop")
    scope = dict(os=os, signal=signal, subprocess=subprocess, socket=socket,
                 errno=errno, time=time)
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), scope)
    with tempfile.TemporaryDirectory() as directory:
        marker = pathlib.Path(directory) / "port"
        child = ("import socket,time,pathlib; s=socket.socket(); "
                 "s.bind(('127.0.0.1',0)); s.listen(); pathlib.Path("
                 + repr(str(marker)) + ").write_text(str(s.getsockname()[1])); time.sleep(60)")
        parent = ("import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',"
                  + repr(child) + "]); time.sleep(60)")
        host = subprocess.Popen([sys.executable, "-c", parent], start_new_session=True)
        try:
            for _ in range(100):
                if marker.exists():
                    break
                time.sleep(.05)
            port = int(marker.read_text())
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                pass
            scope["stop"](host, port)
            assert host.poll() is not None
            with socket.socket() as probe:
                assert probe.connect_ex(("127.0.0.1", port)) == errno.ECONNREFUSED
            scope["stop"](host, port)
            print(name + ": child listener closed; parent reaped; repeated stop succeeds")
        finally:
            try:
                os.killpg(host.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            host.wait(timeout=10)
