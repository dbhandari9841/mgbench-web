#!/usr/bin/bash
set -euo pipefail

MANUAL_SCANNER="/eos/user/d/dbhandar/www/mgbench/bin/scan_manual_benchmarks.py"
STANDALONE_SCANNER="/eos/user/d/dbhandar/www/mgbench/bin/scan_standalone_benchmarks.py"
MANUAL_SRC="/afs/cern.ch/user/d/dbhandar/MANUAL_BENCHMARK_PATCHED"
STANDALONE_SRC="$HOME/MG_standalone_lhe_shower"
OUT="/eos/user/d/dbhandar/www/mgbench/data/manual_benchmarks.json"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

python3 "$MANUAL_SCANNER" "$MANUAL_SRC" > "$TMPDIR/manual.json"
python3 "$STANDALONE_SCANNER" "$STANDALONE_SRC" > "$TMPDIR/standalone.json"

python3 - "$TMPDIR/manual.json" "$TMPDIR/standalone.json" > "$TMPDIR/combined.json" <<'PY'
import json, sys
manual = json.load(open(sys.argv[1]))
standalone = json.load(open(sys.argv[2]))
rows = manual + standalone
rows.sort(key=lambda r: ((r.get("created") is None), str(r.get("created"))), reverse=True)
json.dump(rows, sys.stdout, separators=(",", ":"))
PY

mv "$TMPDIR/combined.json" "$OUT"
echo "Wrote combined cache to $OUT"