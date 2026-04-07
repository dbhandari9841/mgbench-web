#!/usr/bin/env python3
import cgi
import cgitb
import html
import os
import re
import uuid
from datetime import datetime

cgitb.enable()

print("Content-Type: text/html")
print()

def fail(msg: str) -> None:
    print("<html><body>")
    print("<h1>Submission failed</h1>")
    print(f"<p>{html.escape(msg)}</p>")
    print('<p><a href="/mgbench/">Back</a></p>')
    print("</body></html>")
    raise SystemExit

form = cgi.FieldStorage()

process = form.getfirst("process", "DY").strip()
jets = form.getfirst("jets", "0").strip()
mode = form.getfirst("mode", "inclusive").strip()
version = form.getfirst("version", "3.6.7").strip()
backend = form.getfirst("backend", "cuda").strip()
nevt = form.getfirst("nevt", "1000").strip()

allowed_process = {"DY", "TT"}
allowed_jets = {"0", "1", "2", "3", "3_simplified", "4_simplified"}
allowed_mode = {"inclusive", "exclusive"}
allowed_version = {"3.6.7", "3.6.7_dev", "3.6.7_stable"}
allowed_backend = {"fortran", "cppnone", "cppavx2", "cuda"}

if process not in allowed_process:
    fail(f"Invalid process: {process}")
if jets not in allowed_jets:
    fail(f"Invalid jets option: {jets}")
if mode not in allowed_mode:
    fail(f"Invalid mode: {mode}")
if version not in allowed_version:
    fail(f"Invalid version: {version}")
if backend not in allowed_backend:
    fail(f"Invalid backend: {backend}")
if not re.fullmatch(r"\d+", nevt):
    fail("Events must be an integer")

nevt_int = int(nevt)
if nevt_int < 100 or nevt_int > 5_000_000:
    fail("Events out of allowed range")

card_map = {
    ("TT", "0", "exclusive"): "tt_0j_exclusive",
    ("TT", "1", "exclusive"): "tt_1j_exclusive",
    ("TT", "2", "exclusive"): "tt_2j_exclusive",
    ("TT", "3", "exclusive"): "tt_3j_exclusive",

    ("TT", "0", "inclusive"): "tt_0j_inclusive",
    ("TT", "1", "inclusive"): "tt_1j_inclusive",
    ("TT", "2", "inclusive"): "tt_2j_inclusive",
    ("TT", "3", "inclusive"): "tt_3j_inclusive",

    ("DY", "0", "exclusive"): "dy_0j_exclusive",
    ("DY", "1", "exclusive"): "dy_1j_exclusive",
    ("DY", "2", "exclusive"): "dy_2j_exclusive",
    ("DY", "3", "exclusive"): "dy_3j_exclusive",

    ("DY", "0", "inclusive"): "dy_0j_inclusive",
    ("DY", "1", "inclusive"): "dy_1j_inclusive",
    ("DY", "2", "inclusive"): "dy_2j_inclusive",
    ("DY", "3", "inclusive"): "dy_3j_inclusive",

    ("DY", "3_simplified", "exclusive"): "dy_3j_simplified",
    ("DY", "4_simplified", "exclusive"): "dy_4j_simplified",
    ("DY", "3_simplified", "inclusive"): "dy_3j_simplified",
    ("DY", "4_simplified", "inclusive"): "dy_4j_simplified",
}

key = (process, jets, mode)
if key not in card_map:
    fail(f"No CARD_KEY mapping exists for {key}")

card_key = card_map[key]
request_id = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:10]}"

request_dir = "/eos/user/d/dbhandar/www/mgbench/jobs/requests"

if not os.path.isdir(request_dir):
    fail(f"Request directory does not exist: {request_dir}")

try:
    probe = os.path.join(request_dir, ".cgi_write_probe")
    with open(probe, "w") as f:
        f.write("ok\n")
    os.remove(probe)
except OSError as e:
    fail(f"Request directory is not writable by CGI: {request_dir} ({e})")

request_record = os.path.join(request_dir, f"{request_id}.txt")

payload = (
    f'request_id="{request_id}"\n'
    f'process="{process}"\n'
    f'jets="{jets}"\n'
    f'mode="{mode}"\n'
    f'version="{version}"\n'
    f'backend="{backend}"\n'
    f'nevt="{nevt_int}"\n'
    f'card_key="{card_key}"\n'
)

try:
    with open(request_record, "x") as f:
        f.write(payload)
except FileExistsError:
    fail(f"Request file already exists unexpectedly: {request_record}")
except OSError as e:
    fail(f"Could not write request file: {e}")

print("<html><body>")
print("<h1>Benchmark request queued</h1>")
print("<ul>")
print(f"<li><b>Request ID:</b> {html.escape(request_id)}</li>")
print(f"<li><b>Process:</b> {html.escape(process)}</li>")
print(f"<li><b>Jets:</b> {html.escape(jets)}</li>")
print(f"<li><b>Mode:</b> {html.escape(mode)}</li>")
print(f"<li><b>Version:</b> {html.escape(version)}</li>")
print(f"<li><b>Backend:</b> {html.escape(backend)}</li>")
print(f"<li><b>Events:</b> {nevt_int}</li>")
print(f"<li><b>CARD_KEY:</b> {html.escape(card_key)}</li>")
print("</ul>")
print(f"<p>Saved request file: <code>{html.escape(request_record)}</code></p>")
print("<p>Your relay will pick this up automatically.</p>")
print(f'<p><a href="/mgbench/cgi-bin/status.py?id={html.escape(request_id)}">Track this request</a></p>')
print('<p><a href="/mgbench/">Back to form</a></p>')
print("</body></html>")