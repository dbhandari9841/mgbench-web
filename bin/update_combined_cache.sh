#!/usr/bin/bash
set -euo pipefail

MANUAL_SCANNER="/eos/user/d/dbhandar/www/mgbench/bin/scan_manual_benchmarks.py"
STANDALONE_SCANNER="/eos/user/d/dbhandar/www/mgbench/bin/scan_standalone_benchmarks.py"
PLOT_BUILDER="/eos/user/d/dbhandar/www/mgbench/bin/build_plot_data.py"

MANUAL_SRC="/afs/cern.ch/user/d/dbhandar/MANUAL_BENCHMARK_PATCHED"
STANDALONE_SRC="${HOME}/MG_standalone_lhe_shower"

DATA_DIR="/eos/user/d/dbhandar/www/mgbench/data"
COMBINED_OUT="${DATA_DIR}/manual_benchmarks.json"
PLOT_OUT="${DATA_DIR}/plot_data.json"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

python3 "$MANUAL_SCANNER" "$MANUAL_SRC" > "$TMPDIR/manual.json"
python3 "$STANDALONE_SCANNER" "$STANDALONE_SRC" > "$TMPDIR/standalone.json"

python3 - "$TMPDIR/manual.json" "$TMPDIR/standalone.json" > "$TMPDIR/combined.json" <<'PY2'
import json
import sys

manual = json.load(open(sys.argv[1], encoding='utf-8'))
standalone = json.load(open(sys.argv[2], encoding='utf-8'))
rows = manual + standalone
rows.sort(key=lambda r: ((r.get('created') is None), str(r.get('created'))), reverse=True)
json.dump(rows, sys.stdout, separators=(",", ":"))
PY2

mv "$TMPDIR/combined.json" "$COMBINED_OUT"
echo "Wrote combined cache to $COMBINED_OUT"

if [ -x "$PLOT_BUILDER" ]; then
  python3 "$PLOT_BUILDER" "$COMBINED_OUT" > "$TMPDIR/plot_data.json"
  mv "$TMPDIR/plot_data.json" "$PLOT_OUT"
  echo "Wrote plot data to $PLOT_OUT"
else
  echo "Skipping plot data build: $PLOT_BUILDER not found or not executable"
fi