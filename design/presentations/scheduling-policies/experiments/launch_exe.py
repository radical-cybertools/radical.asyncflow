"""Launcher for diag_launch.py's Batch executables: times the child and writes the times.

Usage: python launch_exe.py <out.json> <command...>
"""

import json
import os
import subprocess
import sys
import time

t0 = time.time()
rc = subprocess.run(sys.argv[2:]).returncode
t1 = time.time()
with open(sys.argv[1] + ".tmp", "w") as f:
    json.dump({"t0": t0, "t1": t1, "rc": rc, "pid": os.getpid()}, f)
os.replace(sys.argv[1] + ".tmp", sys.argv[1])
sys.exit(rc)
