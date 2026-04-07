#!/usr/bin/env python3
import cgi
import cgitb
import html
import json
import os

cgitb.enable()

print("Content-Type: text/html")
print()

form = cgi.FieldStorage()
request_id = form.getfirst("id", "").strip()

BASE = "/eos/user/d/dbhandar/www/mgbench/jobs/results"

print("<html><head>")
if request_id:
    print('<meta http-equiv="refresh" content="5">')
print("</head><body>")

if not request_id:
    print("<h1>Missing request ID</h1>")
    print("</body></html>")
    raise SystemExit

status_file = os.path.join(BASE, f"{request_id}.status.json")
result_file = os.path.join(BASE, f"{request_id}.result.json")

print(f"<h1>Status for {html.escape(request_id)}</h1>")

if os.path.exists(status_file):
    with open(status_file) as f:
        status = json.load(f)
    print("<h2>Status</h2><pre>")
    print(html.escape(json.dumps(status, indent=2)))
    print("</pre>")
else:
    print("<p>No status file yet. Request may still be waiting to be processed.</p>")

if os.path.exists(result_file):
    with open(result_file) as f:
        result = json.load(f)
    print("<h2>Result</h2><pre>")
    print(html.escape(json.dumps(result, indent=2)))
    print("</pre>")

print('<p><a href="/mgbench/">Back</a></p>')
print("</body></html>")