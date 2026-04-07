#!/usr/bin/env python3
import os
import pwd
import subprocess
import traceback

path = "/usr/bin/condor_submit"

print("Content-Type: text/plain")
print()

print("=== identity ===")
print("uid:", os.getuid())
print("user:", pwd.getpwuid(os.getuid()).pw_name)
print("cwd:", os.getcwd())

print("\n=== file checks ===")
print("exists:", os.path.exists(path))
print("isfile:", os.path.isfile(path))
print("x_ok:", os.access(path, os.X_OK))

print("\n=== try running condor_submit -version ===")
try:
    p = subprocess.run(
        [path, "-version"],
        capture_output=True,
        text=True,
        timeout=10
    )
    print("returncode:", p.returncode)
    print("--- stdout ---")
    print(p.stdout)
    print("--- stderr ---")
    print(p.stderr)
except Exception as e:
    print("EXCEPTION:", repr(e))
    print(traceback.format_exc())
