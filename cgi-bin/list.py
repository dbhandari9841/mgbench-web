#!/usr/bin/env python3
import os
import html

BASE = "/eos/user/d/dbhandar/www/mgbench/jobs/results"

print("Content-Type: text/html")
print()

print("<html><body>")
print("<h1>Recent requests</h1>")

items = []
if os.path.isdir(BASE):
    for name in os.listdir(BASE):
        if name.endswith(".status.json"):
            path = os.path.join(BASE, name)
            reqid = name[:-12]  # strip ".status.json"
            items.append((os.path.getmtime(path), reqid))

items.sort(reverse=True)

print("<ul>")
for _, reqid in items[:50]:
    esc = html.escape(reqid)
    print(f'<li><a href="/mgbench/cgi-bin/status.py?id={esc}">{esc}</a></li>')
print("</ul>")

print('<p><a href="/mgbench/">Back</a></p>')
print("</body></html>")