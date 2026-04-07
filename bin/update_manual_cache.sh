#!/usr/bin/bash
set -euo pipefail

SCANNER="/eos/user/d/dbhandar/www/mgbench/bin/scan_manual_benchmarks.py"
SRC="/afs/cern.ch/user/d/dbhandar/MANUAL_BENCHMARK_PATCHED"
OUT="/eos/user/d/dbhandar/www/mgbench/data/manual_benchmarks.json"
TMP="${OUT}.tmp"

python3 "$SCANNER" "$SRC" > "$TMP"
mv "$TMP" "$OUT"